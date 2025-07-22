use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cmp::min;
use chunkfs::{Data, Database};
use huffman_compress::{Book, CodeBuilder};
use crate::decoder::Decoder;
use crate::hasher::SBCHash;
use crate::{ChunkType, SBCKey, SBCMap};
use crate::chunkfs_sbc::ClusterPoint;
use crate::encoder::{count_delta_chunks_with_hash, get_parent_data, Encoder};
use crate::encoder::zdelta_match_pointers::{MatchPointers, ReferencePointerType};
use crate::encoder::zdelta_comprassion_error::{DataConversionError, StorageError, MatchEncodingError};

/// Threshold for applying penalty to large offsets during match selection.
const LARGE_OFFSET_PENALTY_THRESHOLD: i32 = 4096;
/// Minimum allowed match length (3 bytes).
const MIN_MATCH_LENGTH: usize = 3;
/// Maximum allowed match length (1026 bytes).
const MAX_MATCH_LENGTH: usize = 1026;
/// Block size used for length coefficient calculation.
const LENGTH_BLOCK_SIZE: usize = 256;
/// Maximum coefficient value for match length encoding.
const MAX_LENGTH_COEFFICIENT: u8 = 3;

/// A 3-byte sequence used for finding matches.
type Triplet = [u8; 3];
/// Hash value for a triplet.
type TripletHash = u32;
/// Position within a data chunk.
type PositionInChunk = usize;
/// Collection of positions where a triplet occurs.
type TripletLocations = Vec<PositionInChunk>;

/// Zdelta compression encoder.
///
/// Implements delta compression between target and reference data using:
/// - LZ77-style matching with reference pointers.
/// - Optional Huffman encoding of the delta.
pub struct ZdeltaEncoder {
    /// Whether to use Huffman encoding for the output delta.
    use_huffman_encoding: bool,
}

impl Default for ZdeltaEncoder {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Encoder for ZdeltaEncoder {
    /// Encodes a cluster of data chunks using Zdelta compression against a parent chunk.
    ///
    /// # Arguments
    /// * `target_map` - Thread-safe reference to the chunk storage map (Arc<Mutex>).
    /// * `cluster` - Mutable slice of ClusterPoints to process.
    /// * `parent_hash` - Hash of the suggested parent chunk for delta reference.
    ///
    /// # Returns
    /// A tuple containing:
    /// 1. `usize` - Total bytes of data that couldn't be delta-encoded (left as-is).
    /// 2. `usize` - Total bytes of processed delta-encoded data.
    fn encode_cluster<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        cluster: &mut [ClusterPoint<Hash>],
        parent_hash: Hash
    ) -> (usize, usize) {
        let parent_info = get_parent_data(target_map.clone(), parent_hash.clone(), cluster);
        let mut data_left = parent_info.data_left;
        let mut total_processed_bytes = 0;
        let parent_data = parent_info.parent_data;
        let parent_triplet_lookup_table = match build_triplet_lookup_table(&parent_data) {
            Ok(triplet_lookup_table) => triplet_lookup_table,
            Err(_) => {
                panic!("Chunk is too small (Chunk size should be at least three bytes)")
            }
        };

        for (chunk_id, (hash, data_container)) in cluster.iter_mut().enumerate() {
            if parent_info.index > -1 && chunk_id == parent_info.index as usize {
                continue;
            }
            let mut target_hash = SBCKey::default();
            match data_container.extract() {
                Data::Chunk(data) => {
                    let (left, processed, sbc_hash) = self.encode_delta_chunk(
                        target_map.clone(),
                        data,
                        hash.clone(),
                        parent_data.as_slice(),
                        &parent_triplet_lookup_table,
                        parent_hash.clone(),
                    );
                    data_left += left;
                    total_processed_bytes += processed;
                    target_hash = sbc_hash;
                }
                Data::TargetChunk(_) => {}
            }
            data_container.make_target(vec![target_hash]);
        }
        (data_left, total_processed_bytes)
    }
}

impl ZdeltaEncoder {
    /// Creates a new ZdeltaEncoder.
    ///
    /// # Arguments
    /// * `use_huffman_encoding` - Whether to use Huffman encoding for the delta.
    pub fn new(use_huffman_encoding: bool) -> Self {
        Self { use_huffman_encoding }
    }

    /// Encodes a single data chunk using delta compression against a reference.
    ///
    /// # Arguments
    /// * `target_map` - Shared map for storing compressed chunks.
    /// * `target_data` - The data to be compressed.
    /// * `target_hash` - Hash identifier for the target data.
    /// * `parent_data` - Reference data to compare against.
    /// * `parent_triplet_lookup_table` - Precomputed positions of triplets in reference data.
    /// * `parent_hash` - Hash identifier for the parent/reference data.
    ///
    /// # Returns
    /// 1. Number of uncompressed bytes.
    /// 2. Total bytes processed.
    /// 3. Storage key for the compressed delta.
    ///
    /// # Errors
    /// Returns `ZdeltaCompressionError` if:
    /// - Huffman encoding fails when enabled.
    /// - Match parameters are invalid.
    /// - Storage operations fail.
    fn encode_delta_chunk<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        target_data: &[u8],
        target_hash: Hash,
        parent_data: &[u8],
        parent_triplet_lookup_table: &HashMap<TripletHash, TripletLocations>,
        parent_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
        let mut delta_code : Vec<u8> = Vec::new();
        let mut uncompressed_data = 0;
        let mut pointers = MatchPointers::new(0, 0, 0);

        let huffman_book = if self.use_huffman_encoding {
            Some(create_default_huffman_book())
        } else {
            None
        };

        let mut i : PositionInChunk = 0;
        while i + MIN_MATCH_LENGTH <= target_data.len() {
            let mut triplet = [0u8; 3];
            triplet.copy_from_slice(&target_data[i..i+3]);
            let hash = compute_triplet_hash(&triplet);

            if let Some(parent_positions) = parent_triplet_lookup_table.get(&hash) {
                if let Some((length, offset, pointer_type)) =
                    select_best_match(target_data, parent_data, i, parent_positions, &pointers) {
                    if let Some(book) = huffman_book.as_ref() {
                        match encode_match_huffman(length, offset, &pointer_type, book) {
                            Ok(encoded) => {
                                delta_code.extend_from_slice(&encoded);
                            }
                            Err(_) => {
                                log::warn!("Invalid match length \
                                (allowed: {MIN_MATCH_LENGTH}-{MAX_MATCH_LENGTH}), \
                                falling back to literal encoding");

                                for &byte in &target_data[i..i+length] {
                                    self.encode_literal(
                                        byte,
                                        &huffman_book,
                                        &mut delta_code,
                                        &mut uncompressed_data
                                    );
                                }
                            }
                        }
                    } else {
                        match encode_match_raw(length, offset, &pointer_type) {
                            Ok(encoded) => delta_code.extend_from_slice(&encoded),
                            Err(e) => {
                                match e {
                                    MatchEncodingError::InvalidLength(..) => {
                                        log::warn!("Invalid match length \
                                        (allowed: {MIN_MATCH_LENGTH}-{MAX_MATCH_LENGTH}), \
                                        falling back to literal encoding");
                                    }
                                    MatchEncodingError::InvalidParameterCombination => {
                                        log::error!(
                                        "Invalid parameter combination \
                                        (length: {length}, offset: {offset}, pointer: {pointer_type:?})");
                                    }
                                    _ => log::error!("Unhandled match encoding error: {e:?}"),
                                }
                                for &byte in &target_data[i..i+length] {
                                    delta_code.push(byte);
                                    uncompressed_data += 1;
                                }
                            }
                        }
                    }

                    pointers.update_after_match(i + length, offset, pointer_type);
                    i += length;
                    continue;
                }
            }

            self.encode_literal(
                target_data[i],
                &huffman_book,
                &mut delta_code,
                &mut uncompressed_data
            );
            i += 1;
        }

        while i < target_data.len() {
            self.encode_literal(
                target_data[i],
                &huffman_book,
                &mut delta_code,
                &mut uncompressed_data
            );
            i += 1;
        }

        let sbc_key = match store_delta_chunk(target_map, target_hash, parent_hash, delta_code) {
            Ok(key) => key,
            Err(StorageError::LockFailed(e)) => {
                panic!("Critical storage lock failure: {e}");
            },
            Err(StorageError::InsertionFailed(e)) => {
                panic!("Non-critical insertion failure: {e}");
            }
        };

        (uncompressed_data, target_data.len(), sbc_key)
    }

    /// Encodes a single literal byte using configured encoding.
    ///
    /// # Arguments
    /// * `byte` - The byte to encode.
    /// * `huffman_book` - Huffman code book (when Huffman encoding is enabled).
    /// * `delta_code` - Output buffer for encoded data.
    /// * `uncompressed_data` - Counter for tracking uncompressed bytes.
    ///
    /// # Errors
    /// Returns `MatchEncodingError` if:
    /// - Huffman encoding is enabled but book is not available.
    /// - Huffman encoding fails.
    fn encode_literal(
        &self,
        byte: u8,
        huffman_book: &Option<Book<u8>>,
        delta_code: &mut Vec<u8>,
        uncompressed_data: &mut usize,
    ) {
        if let Some(book) = huffman_book.as_ref() {
            let encoded = encode_literal_huffman(
                byte,
                book
            );
            delta_code.extend_from_slice(&encoded);
        } else {
            delta_code.push(byte);
        }
        *uncompressed_data += 1;
    }
}

/// Stores a compressed delta chunk in the target map.
fn store_delta_chunk<D: Decoder, Hash: SBCHash>(
    target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
    target_hash: Hash,
    parent_hash: Hash,
    delta_code: Vec<u8>,
) -> Result<SBCKey<Hash>, StorageError> {
    let mut target_map_lock = target_map.lock().map_err(|e| {
        StorageError::LockFailed(format!("Failed to acquire lock: {e}"))
    })?;
    let number_delta_chunk = count_delta_chunks_with_hash(&target_map_lock, &target_hash);
    let sbc_hash = SBCKey {
        hash: target_hash,
        chunk_type: ChunkType::Delta {
            parent_hash,
            number: number_delta_chunk,
        },
    };

    target_map_lock.insert(sbc_hash.clone(), delta_code).map_err(|e| {
        StorageError::InsertionFailed(format!("Failed to insert delta chunk: {e}"))
    })?;

    Ok(sbc_hash)
}

/// Encodes a match using Huffman coding.
///
/// # Arguments
/// * `match_length` - Length of match (3-1026 bytes).
/// * `offset` - Signed offset from reference pointer (-32768..32767).
/// * `pointer_type` - Which reference pointer was used.
/// * `book` - Huffman code book for encoding.
///
/// # Returns encoded bytes representing the match or error if:
/// - Match length is out of valid range.
/// - Huffman encoding fails.
///
/// # Encoding Format
/// The match is encoded as:
/// 1. Flag byte (combines length coefficient, pointer type and direction).
/// 2. Length remainder.
/// 3. Offset bytes (big-endian).
fn encode_match_huffman(
    match_length: usize,
    offset: i16,
    pointer_type: &ReferencePointerType,
    book: &Book<u8>,
) -> Result<Vec<u8>, MatchEncodingError> {
    if !(MIN_MATCH_LENGTH..=MAX_MATCH_LENGTH).contains(&match_length) {
        return Err(MatchEncodingError::InvalidLength(
            match_length,
            MIN_MATCH_LENGTH,
            MAX_MATCH_LENGTH,
        ));
    }

    let (length_remainder, length_coefficient) = calculate_length_components(match_length);
    let is_positive_offset = offset >= 0;

    let flag = encode_match_flag(length_coefficient, pointer_type, is_positive_offset)?;

    let offset_abs = offset.unsigned_abs();
    let [offset_high, offset_low] = offset_abs.to_be_bytes();

    use bit_vec::BitVec;
    let mut buffer = BitVec::new();

    book.encode(&mut buffer, &flag).expect("Flag codes (1-20) must be in codebook");
    book.encode(&mut buffer, &length_remainder).expect("Length remainders (0-255) must be in codebook");
    book.encode(&mut buffer, &offset_high).expect("Offset bytes (0-255) must be in codebook");
    book.encode(&mut buffer, &offset_low).expect("Offset bytes (0-255) must be in codebook");

    Ok(buffer.to_bytes())
}

/// Creates default Huffman coding book optimized for zdelta.
///
/// The book contains codes for:
/// - 20 flag values.
/// - 256 literal bytes.
/// - 256 length remainders.
/// - 256 offset bytes.
///
/// Frequencies are weighted to favor:
/// - Smaller flag values.
/// - ASCII literals.
/// - Smaller lengths and offsets.
fn create_default_huffman_book() -> Book<u8> {
    let mut frequencies = HashMap::new();

    // Frequencies for flags (1-20)
    for i in 1..=20 {
        frequencies.insert(i as u8, 100);
    }

    // Frequencies for literals (0-255)
    for i in 0..=255 {
        frequencies.insert(i as u8, if i < 128 { 50 } else { 10 });
    }

    // Frequencies for length residues (0-255)
    for i in 0..=255 {
        frequencies.insert(i as u8, if i < 128 { 30 } else { 5 });
    }

    // Frequencies for offsets (0-255)
    for i in 0..=255 {
        frequencies.insert(i as u8, if i < 128 { 20 } else { 5 });
    }

    let (book, _) = CodeBuilder::from_iter(frequencies).finish();
    book
}

/// Encodes a literal byte using Huffman coding.
///
/// # Arguments
/// * `literal` - The byte value to encode.
/// * `book` - Huffman code book for encoding.
///
/// # Returns
/// Encoded bytes or error if encoding fails.
fn encode_literal_huffman(
    literal: u8,
    book: &Book<u8>,
) -> Vec<u8> {
    use bit_vec::BitVec;
    let mut buffer = BitVec::new();

    book.encode(&mut buffer, &literal).expect("All literals (0-255) must be in codebook");

    buffer.to_bytes()
}

/// Encodes a match using raw byte representation (without Huffman coding).
///
/// # Arguments
/// * `match_length` - Length of the match (3-1026 bytes).
/// * `offset` - Signed offset from reference pointer.
/// * `pointer_type` - Which reference pointer was used.
///
/// # Encoding Format
/// 1. Flag byte.
/// 2. Length remainder byte.
/// 3. Offset high byte.
/// 4. Offset low byte.
fn encode_match_raw(
    match_length: usize,
    offset: i16,
    pointer_type: &ReferencePointerType
) -> Result<Vec<u8>, MatchEncodingError> {
    if !(MIN_MATCH_LENGTH..=MAX_MATCH_LENGTH).contains(&match_length) {
        return Err(MatchEncodingError::InvalidLength(
            match_length,
            MIN_MATCH_LENGTH,
            MAX_MATCH_LENGTH,
        ));
    }

    let (length_remainder, length_coefficient) = calculate_length_components(match_length);
    let is_positive_offset = offset >= 0;

    let flag = encode_match_flag(length_coefficient, pointer_type, is_positive_offset)?;

    let offset_abs = offset.unsigned_abs();
    let [offset_high, offset_low] = offset_abs.to_be_bytes();

    Ok(vec![flag, length_remainder, offset_high, offset_low])
}

/// Calculates length components for match encoding.
///
/// Splits match length into:
/// - Remainder (0-255).
/// - Coefficient (0-3).
///
/// # Returns
/// Tuple of (remainder, coefficient).
fn calculate_length_components(match_length: usize) -> (u8, u8) {
    let effective_length = match_length.max(MIN_MATCH_LENGTH) - MIN_MATCH_LENGTH;

    let length_coefficient = (effective_length / LENGTH_BLOCK_SIZE) as u8;
    let length_remainder = (effective_length % LENGTH_BLOCK_SIZE) as u8;

    if length_coefficient >= MAX_LENGTH_COEFFICIENT {
        (255, MAX_LENGTH_COEFFICIENT)
    } else {
        (length_remainder, length_coefficient)
    }
}

/// Encodes match flag combining length coefficient, pointer type and direction.
///
/// # Arguments
/// * `length_coefficient` - Length coefficient (0-3).
/// * `pointer_type` - Which pointer was used.
/// * `is_positive_offset` - Whether offset is positive.
///
/// # Returns
/// Encoded flag byte or error for invalid combination.
///
/// # Flag Encoding
/// Each unique combination maps to a value 1-20:
/// - First 5 values: coefficient 0.
/// - Next 5: coefficient 1.
/// - Next 5: coefficient 2.
/// - Last 5: coefficient 3.
fn encode_match_flag(
    length_coefficient: u8,
    pointer_type: &ReferencePointerType,
    is_positive_offset: bool,
) -> Result<u8, MatchEncodingError> {
    match (length_coefficient, pointer_type, is_positive_offset) {
        (0, ReferencePointerType::TargetLocal, _) => Ok(1),
        (0, ReferencePointerType::Main, true) => Ok(2),
        (0, ReferencePointerType::Main, false) => Ok(3),
        (0, ReferencePointerType::Auxiliary, true) => Ok(4),
        (0, ReferencePointerType::Auxiliary, false) => Ok(5),
        (1, ReferencePointerType::TargetLocal, _) => Ok(6),
        (1, ReferencePointerType::Main, true) => Ok(7),
        (1, ReferencePointerType::Main, false) => Ok(8),
        (1, ReferencePointerType::Auxiliary, true) => Ok(9),
        (1, ReferencePointerType::Auxiliary, false) => Ok(10),
        (2, ReferencePointerType::TargetLocal, _) => Ok(11),
        (2, ReferencePointerType::Main, true) => Ok(12),
        (2, ReferencePointerType::Main, false) => Ok(13),
        (2, ReferencePointerType::Auxiliary, true) => Ok(14),
        (2, ReferencePointerType::Auxiliary, false) => Ok(15),
        (3, ReferencePointerType::TargetLocal, _) => Ok(16),
        (3, ReferencePointerType::Main, true) => Ok(17),
        (3, ReferencePointerType::Main, false) => Ok(18),
        (3, ReferencePointerType::Auxiliary, true) => Ok(19),
        (3, ReferencePointerType::Auxiliary, false) => Ok(20),
        _ => Err(MatchEncodingError::InvalidParameterCombination),
    }
}

/// Selects the best match from possible candidate positions.
///
/// Uses scoring system that considers both match length and offset:
/// - Longer matches score higher.
/// - Smaller offsets score higher.
/// - Large offsets (>4096) get length penalty.
///
/// # Arguments
/// * `target_data` - Data being compressed.
/// * `parent_data` - Reference data.
/// * `current_position` - Position in target data.
/// * `parent_positions` - Candidate match positions in reference.
/// * `pointers` - Current pointer positions.
///
/// # Returns
/// Best match (length, offset, pointer_type) or None if no good matches.
fn select_best_match(
    target_data: &[u8],
    parent_data: &[u8],
    current_position: usize,
    parent_positions: &[usize],
    pointers: &MatchPointers,
) -> Option<(usize, i16, ReferencePointerType)> {
    const SCORE_LENGTH_SHIFT: usize = 16;
    const MAX_SCORE_OFFSET: usize = 0xFFFF;

    let mut best_match = None;
    let mut best_score = 0;

    for &parent_position in parent_positions {
        if let Some(length) = find_max_match_length(target_data, parent_data, current_position, parent_position) {
            let (offset, pointer_type) = pointers.calculate_offset(parent_position);

            let adjusted_length = if offset.abs() > LARGE_OFFSET_PENALTY_THRESHOLD as i16 {
                length.saturating_sub(1)
            } else {
                length
            };

            let score = (adjusted_length << SCORE_LENGTH_SHIFT) | (!offset.abs() as usize & MAX_SCORE_OFFSET);

            if score > best_score {
                best_score = score;
                best_match = Some((length, offset, pointer_type));
            }
        }
    }

    best_match
}

/// Finds the longest match between target and reference data at given positions.
///
/// # Arguments
/// * `target_data` - Data being compressed.
/// * `parent_data` - Reference data.
/// * `start_position_in_target` - Start position in target data.
/// * `start_position_in_parent` - Start position in reference data.
///
/// # Returns
/// Length of longest match (at least MIN_MATCH_LENGTH) or None if:
/// - Positions are out of bounds.
/// - Initial triplet doesn't match.
/// - No match of minimum length found.
fn find_max_match_length(
    target_data: &[u8],
    parent_data: &[u8],
    start_position_in_target: PositionInChunk,
    start_position_in_parent: PositionInChunk,
) -> Option<usize> {
    if start_position_in_target + MIN_MATCH_LENGTH > target_data.len() ||
        start_position_in_parent + MIN_MATCH_LENGTH > parent_data.len() ||
        target_data[start_position_in_target..start_position_in_target + MIN_MATCH_LENGTH] !=
            parent_data[start_position_in_parent..start_position_in_parent + MIN_MATCH_LENGTH] {
        return None;
    }

    let max_possible_match_length = min(
        parent_data.len() - start_position_in_parent,
        target_data.len() - start_position_in_target,
    ).min(MAX_MATCH_LENGTH);

    let mut match_length = MIN_MATCH_LENGTH;
    while match_length < max_possible_match_length
    && target_data[start_position_in_target + match_length] == parent_data[start_position_in_parent + match_length] {
        match_length += 1;
    }
    Some(match_length)
}

/// Computes hash value for a 3-byte sequence.
fn compute_triplet_hash(triplet: &Triplet) -> TripletHash {
    ((triplet[0] as u32) << 16) | ((triplet[1] as u32) << 8) | triplet[2] as u32
}

/// Builds lookup table mapping triplets to their positions in data.
///
/// # Returns
/// Hash map of triplet hashes to positions or error if data too small
fn build_triplet_lookup_table(chunk: &[u8]) -> Result<HashMap<TripletHash, TripletLocations>, DataConversionError> {
    if chunk.len() < MIN_MATCH_LENGTH {
        return Err(DataConversionError::ChunkTooSmall {
            actual_size: chunk.len(),
            required_size: MIN_MATCH_LENGTH,
        });
    }

    let mut lookup_table : HashMap<TripletHash, TripletLocations> = HashMap::new();

    for (current_position, triplet) in chunk.windows(MIN_MATCH_LENGTH).enumerate() {
        let triplet_array = [triplet[0], triplet[1], triplet[2]];
        let hash = compute_triplet_hash(&triplet_array);

        lookup_table
            .entry(hash)
            .or_default()
            .push(current_position);
    }

    Ok(lookup_table)
}

#[cfg(test)]
mod tests {
    use huffman_compress::Book;
    use bit_vec::BitVec;
    use super::*;

    #[test]
    fn encode_match_huffman_should_encode_valid_match_correctly() {
        let book = create_test_huffman_book();

        let test_cases = vec![
            (3, 100, ReferencePointerType::TargetLocal, false),
            (258, 32767, ReferencePointerType::Main, true),
            (1026, 100, ReferencePointerType::Auxiliary, false),
            (128, 4096, ReferencePointerType::Main, false),
        ];

        for (length, offset, pointer_type, _) in test_cases {
            let result = encode_match_huffman(
                length,
                offset as i16,
                &pointer_type,
                &book
            );

            assert!(result.is_ok(), "Failed to encode length {length}, offset {offset}");
            let encoded = result.unwrap();
            assert!(!encoded.is_empty(), "Encoded data should not be empty");
        }
    }

    #[test]
    fn encode_match_huffman_should_return_error_for_invalid_length() {
        let book = create_test_huffman_book();

        let test_cases = vec![
            (2, 100, ReferencePointerType::Main, true),
            (1027, 100, ReferencePointerType::Main, true),
            (0, 100, ReferencePointerType::Main, true),
        ];

        for (length, offset, pointer_type, _) in test_cases {
            let result = encode_match_huffman(
                length,
                offset as i16,
                &pointer_type,
                &book
            );

            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err(),
                MatchEncodingError::InvalidLength(length, MIN_MATCH_LENGTH, MAX_MATCH_LENGTH)
            );
        }
    }

    #[test]
    fn encode_match_huffman_should_produce_different_output_for_different_inputs() {
        let book = create_test_huffman_book();

        let case1 = encode_match_huffman(
            10, 100, &ReferencePointerType::Main, &book).unwrap();

        let case2 = encode_match_huffman(
            10, 101, &ReferencePointerType::Main, &book).unwrap();

        let case3 = encode_match_huffman(
            11, 100, &ReferencePointerType::Auxiliary, &book).unwrap();

        assert_ne!(case1, case2);
        assert_ne!(case1, case3);
        assert_ne!(case2, case3);
    }

    #[test]
    fn encode_match_huffman_should_handle_edge_cases_correctly() {
        let book = create_test_huffman_book();

        let max_offset = encode_match_huffman(10, 32767, &ReferencePointerType::Main, &book).unwrap();

        let min_offset = encode_match_huffman(10, 0, &ReferencePointerType::Main, &book).unwrap();

        assert!(!max_offset.is_empty());
        assert!(!min_offset.is_empty());
        assert_ne!(max_offset, min_offset);
    }

    #[test]
    fn create_default_huffman_book_should_return_valid_book_for_all_supported_symbols() {
        let book = create_default_huffman_book();

        assert!(!encode_to_bits(&book, 1).is_empty());    // Flag
        assert!(!encode_to_bits(&book, 65).is_empty());   // Literal
        assert!(!encode_to_bits(&book, 200).is_empty());  // Non-ASCII
        assert!(!encode_to_bits(&book, 30).is_empty());   // Length remainder
        assert!(!encode_to_bits(&book, 150).is_empty());  // Offset
    }

    #[test]
    fn create_default_huffman_book_should_assign_shorter_codes_to_more_frequent_symbols() {
        let book = create_default_huffman_book();

        let flag_code_len = encode_to_bits(&book, 1).len();
        let common_literal_len = encode_to_bits(&book, 65).len();
        let rare_literal_len = encode_to_bits(&book, 200).len();

        assert!(flag_code_len < rare_literal_len);
        assert!(common_literal_len < rare_literal_len);
    }

    #[test]
    fn create_default_huffman_book_should_assign_shorter_codes_to_ascii_vs_non_ascii_literals() {
        let book = create_default_huffman_book();

        let ascii_len = encode_to_bits(&book, 65).len();
        let non_ascii_len = encode_to_bits(&book, 200).len();

        assert!(ascii_len <= non_ascii_len);
    }

    #[test]
    fn create_default_huffman_book_should_support_all_possible_byte_values() {
        let book = create_default_huffman_book();

        for i in 0..=255u8 {
            assert!(!encode_to_bits(&book, i).is_empty(), "Failed to encode byte {i}");
        }
    }

    #[test]
    fn create_default_huffman_book_should_produce_different_codes_for_different_inputs() {
        let book = create_default_huffman_book();

        let code1 = encode_to_bits(&book, 1);
        let code2 = encode_to_bits(&book, 2);
        let code65 = encode_to_bits(&book, 65);
        let code200 = encode_to_bits(&book, 200);

        assert_ne!(code1, code2);
        assert_ne!(code1, code65);
        assert_ne!(code1, code200);
        assert_ne!(code65, code200);
    }

    #[test]
    fn encode_match_raw_should_return_correct_encoding_for_basic_match() {
        let result = encode_match_raw(10, 100, &ReferencePointerType::Main);
        assert_eq!(result, Ok(vec![2, 7, 0, 100]));
    }

    #[test]
    fn encode_match_raw_should_handle_negative_offset_correctly() {
        let result = encode_match_raw(300, -1024, &ReferencePointerType::Auxiliary);
        assert_eq!(result, Ok(vec![10, 41, 4, 0]));
    }

    #[test]
    fn encode_match_raw_should_encode_max_values_correctly() {
        let result = encode_match_raw(1026, -32766, &ReferencePointerType::TargetLocal);
        assert_eq!(result, Ok(vec![16, 255, 127, 254]));
    }

    #[test]
    fn encode_match_raw_should_reject_length_below_minimum() {
        let result = encode_match_raw(2, 100, &ReferencePointerType::Main);
        assert_eq!(result, Err(MatchEncodingError::InvalidLength(2, 3, 1026)));
    }

    #[test]
    fn encode_match_raw_should_reject_length_above_maximum() {
        let result = encode_match_raw(2000, 100, &ReferencePointerType::Main);
        assert_eq!(result, Err(MatchEncodingError::InvalidLength(2000, 3, 1026)));
    }

    #[test]
    fn encode_match_flag_should_return_correct_flag_for_target_local() {
        assert_eq!(encode_match_flag(0, &ReferencePointerType::TargetLocal, true), Ok(1));
        assert_eq!(encode_match_flag(1, &ReferencePointerType::TargetLocal, false), Ok(6));
        assert_eq!(encode_match_flag(2, &ReferencePointerType::TargetLocal, true), Ok(11));
        assert_eq!(encode_match_flag(3, &ReferencePointerType::TargetLocal, false), Ok(16));
    }

    #[test]
    fn encode_match_flag_should_return_correct_flag_for_main_pointer() {
        assert_eq!(encode_match_flag(0, &ReferencePointerType::Main, true), Ok(2));
        assert_eq!(encode_match_flag(1, &ReferencePointerType::Main, true), Ok(7));
        assert_eq!(encode_match_flag(2, &ReferencePointerType::Main, false), Ok(13));
        assert_eq!(encode_match_flag(3, &ReferencePointerType::Main, false), Ok(18));
    }

    #[test]
    fn encode_match_flag_should_return_correct_flag_for_auxiliary_pointer() {
        assert_eq!(encode_match_flag(0, &ReferencePointerType::Auxiliary, true), Ok(4));
        assert_eq!(encode_match_flag(1, &ReferencePointerType::Auxiliary, true), Ok(9));
        assert_eq!(encode_match_flag(2, &ReferencePointerType::Auxiliary, false), Ok(15));
        assert_eq!(encode_match_flag(3, &ReferencePointerType::Auxiliary, false), Ok(20));
    }

    #[test]
    fn encode_match_flag_should_return_error_for_invalid_combination() {
        assert_eq!(
            encode_match_flag(4, &ReferencePointerType::Main, true),
            Err(MatchEncodingError::InvalidParameterCombination)
        );
    }

    #[test]
    fn calculate_length_components_should_calculate_correctly_for_min_length() {
        assert_eq!(calculate_length_components(3), (0, 0));
    }

    #[test]
    fn calculate_length_components_should_calculate_correctly_for_mid_range() {
        assert_eq!(calculate_length_components(259), (0, 1));
        assert_eq!(calculate_length_components(514), (255, 1));
    }

    #[test]
    fn calculate_length_components_should_calculate_correctly_for_max_length() {
        assert_eq!(calculate_length_components(1026), (255, 3));
    }

    #[test]
    fn calculate_length_components_should_cap_coefficient_at_max() {
        assert_eq!(calculate_length_components(2000), (255, 3));
    }

    #[test]
    fn select_best_match_should_find_best_match_with_small_offset() {
        let target = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let parent = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK".to_vec();
        let pointers = MatchPointers::new(0, 10, 20);
        let parent_positions = vec![10];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((26, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn select_best_match_should_apply_penalty_for_large_offset() {
        let target = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        let parent = b"012345678#012345678#".repeat(500).to_vec();
        let pointers = MatchPointers::new(0, 0, 10_000);
        let parent_positions = vec![0, 10_000 - 10];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((9, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn select_best_match_should_prefer_closer_match_when_lengths_equal() {
        let target = b"abcdef".to_vec();
        let parent = b"xxabcdefyyabcdefzz".to_vec();
        let pointers = MatchPointers::new(0, 2, 10);
        let parent_positions = vec![2, 10];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((6, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn select_best_match_should_prefer_longer_match_over_closer() {
        let target = b"abcdefgh".to_vec();
        let parent = b"abcdwxyzabcdefghijkl".to_vec();
        let pointers = MatchPointers::new(0, 0, 8);
        let parent_positions = vec![0, 8];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((8, 0, ReferencePointerType::Auxiliary)));
    }

    #[test]
    fn select_best_match_should_use_target_local_for_matches_before_target_ptr() {
        let target = b"abcdef".to_vec();
        let parent = b"abcdef".to_vec();
        let pointers = MatchPointers::new(10, 0, 0);
        let parent_positions = vec![0];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((6, -10, ReferencePointerType::TargetLocal)));
    }

    #[test]
    fn select_best_match_should_return_none_when_no_matches_found() {
        let target = b"abcdef".to_vec();
        let parent = b"ghijkl".to_vec();
        let pointers = MatchPointers::default();
        let parent_positions = vec![0];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, None);
    }

    #[test]
    fn select_best_match_should_handle_min_length_match() {
        let target = b"abc".to_vec();
        let parent = b"xyzabc123".to_vec();
        let pointers = MatchPointers::new(0, 3, 0);
        let parent_positions = vec![3];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((3, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn find_max_match_length_should_return_full_match_length_when_sequences_are_identical() {
        let (parent_data, target_data) = create_test_data_for_find_max_match_length();
        let result = find_max_match_length(target_data, parent_data, 0, 0);
        assert_eq!(result, Some(6));
    }

    #[test]
    fn find_max_match_length_should_return_min_match_length_when_only_triplet_matches() {
        let (parent_data, target_data) = create_test_data_for_find_max_match_length();
        let result = find_max_match_length(target_data, parent_data, 3, 3);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn find_max_match_length_should_return_none_when_triplet_does_not_match() {
        let (parent_data, target_data) = create_test_data_for_find_max_match_length();
        let result = find_max_match_length(target_data, parent_data, 9, 9);
        assert_eq!(result, None);
    }

    #[test]
    fn find_max_match_length_should_respect_max_length_limit() {
        let long_data = vec![b'X'; 2000];
        let result = find_max_match_length(&long_data, &long_data, 0, 0);
        assert_eq!(result, Some(MAX_MATCH_LENGTH));
    }

    #[test]
    fn find_max_match_length_should_handle_edge_cases_safely() {
        assert_eq!(find_max_match_length(b"", b"", 0, 0), None);
        assert_eq!(find_max_match_length(b"a", b"a", 0, 0), None); // Меньше MIN_MATCH_LENGTH
    }

    #[test]
    fn find_max_match_length_should_detect_hash_collisions_correctly() {
        let parent = b"abc"; // Хэш может совпадать с "abd"
        let target = b"abd";
        assert_eq!(find_max_match_length(target, parent, 0, 0), None);
    }

    #[test]
    fn build_triplet_lookup_table_should_handles_duplicate_triplets_correctly() {
        let data = b"abcabcabc";
        let table = build_triplet_lookup_table(data).unwrap();

        assert_eq!(table.len(), 3);

        assert_eq!(
            table.get(&compute_triplet_hash(b"abc")),
            Some(&vec![0, 3, 6])
        );
        assert_eq!(
            table.get(&compute_triplet_hash(b"bca")),
            Some(&vec![1, 4])
        );
        assert_eq!(
            table.get(&compute_triplet_hash(b"cab")),
            Some(&vec![2, 5])
        );
    }

    #[test]
    fn compute_triplet_hash_should_return_correct_hash_for_normal_triplet() {
        let data : Triplet = [1, 2, 3];
        assert_eq!(compute_triplet_hash(&data), 0x010203);
    }

    #[test]
    fn compute_triplet_hash_should_return_correct_hash_for_edge_case_values() {
        assert_eq!(
            compute_triplet_hash(&[0, 0, 0]),
            0x000000
        );
        assert_eq!(
            compute_triplet_hash(&[255, 255, 255]),
            0xFFFFFF
        );
    }

    fn create_test_data_for_find_max_match_length<'a>() -> (&'a [u8], &'a [u8]) {
        let target_data = b"abc123xyzabc";
        let parent_data = b"abc123def456";
        (target_data, parent_data)
    }

    fn encode_to_bits(book: &Book<u8>, symbol: u8) -> BitVec {
        let mut buffer = BitVec::new();
        book.encode(&mut buffer, &symbol).expect("Encoding failed in test");
        buffer
    }

    fn create_test_huffman_book() -> Book<u8> {
        let mut frequencies = HashMap::new();
        for i in 1..=20 {
            frequencies.insert(i, 1);
        }
        for i in 0..=255 {
            frequencies.insert(i, 1);
        }
        CodeBuilder::from_iter(frequencies).finish().0
    }
}