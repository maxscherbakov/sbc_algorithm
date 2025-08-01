mod gdelta_decoder;
mod levenshtein_decoder;
mod zdelta_decoder;

pub use gdelta_decoder::GdeltaDecoder;
pub use levenshtein_decoder::LevenshteinDecoder;
pub use zdelta_decoder::ZdeltaDecoder;
/// A trait for decoding delta codes generated by Similarity Based Chunking.
///
/// Implementors of this trait provide a method to decode a delta code into its original form,
/// given the parent data from which the delta was derived.
pub trait Decoder {
    /// Decodes a delta code into its original form using the provided parent data.
    ///
    /// # Parameters
    /// - `parent_data`: The original data from which the delta code was generated.
    /// - `delta_code`: The delta code to be decoded.
    ///
    /// # Returns
    /// The decoded data in its original form.
    fn decode_chunk(&self, parent_data: Vec<u8>, delta_code: &[u8]) -> Vec<u8>;
}
