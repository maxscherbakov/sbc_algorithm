use crate::encoder::zdelta_comprassion_error::PointerError;

const MAX_MATCH_LENGTH: usize = 1026;
const SMALL_OFFSET_THRESHOLD : i16 = 256;

/// Types of reference pointers used in delta compression.
#[derive(Debug)]
#[derive(PartialEq)]
pub enum ReferencePointerType {
    /// Main reference pointer (primary pointer into reference data).
    Main,
    /// Auxiliary reference pointer (secondary pointer into reference data).
    Auxiliary,
    /// Pointer into the target data (local matches).
    TargetLocal,
}

/// Maintains pointers used for finding matches in delta compression.
///
/// Contains:
/// - A pointer into the target data (for local matches).
/// - Two pointers into reference data (main and auxiliary).
pub struct MatchPointers {
    target_ptr: usize,
    main_ref_ptr: usize,
    auxiliary_ref_ptr: usize,
}

impl MatchPointers {
    /// Creates new MatchPointers with specified initial positions.
    pub fn new(target_ptr: usize, main_ref_ptr: usize, auxiliary_ref_ptr: usize) -> Self {
        MatchPointers { target_ptr, main_ref_ptr, auxiliary_ref_ptr }
    }

    /// Calculates the offset from the nearest pointer to the given position.
    ///
    /// Returns:
    /// - The calculated offset (signed).
    /// - The pointer type that was used (which pointer was closest).
    pub fn calculate_offset(&self, parent_position: usize) -> (i16, ReferencePointerType) {
        if parent_position < self.target_ptr {
            let offset = parent_position as i16 - self.target_ptr as i16;
            return (offset, ReferencePointerType::TargetLocal);
        }

        let offset_main = parent_position as i16 - self.main_ref_ptr as i16;
        let offset_auxiliary = parent_position as i16 - self.auxiliary_ref_ptr as i16;

        if offset_main.abs() <= offset_auxiliary.abs() {
            (offset_main, ReferencePointerType::Main)
        } else {
            (offset_auxiliary, ReferencePointerType::Auxiliary)
        }
    }

    /// Updates the pointers after a match has been found.
    ///
    /// According to zdelta's strategy:
    /// - For small offsets (< SMALL_OFFSET_THRESHOLD), moves the pointer that was used.
    /// - For large offsets, moves the other pointer.
    /// - Target pointer is always moved to match end position.
    ///
    /// # Errors
    /// Returns `PointerError::PositionOverflow` if match_end_position would cause overflow.
    pub fn update_after_match(&mut self, match_end_position: usize, offset: i16, pointer_type: ReferencePointerType) -> Result<(), PointerError> {
        if match_end_position > usize::MAX - MAX_MATCH_LENGTH {
            return Err(PointerError::PositionOverflow);
        }

        match pointer_type {
            ReferencePointerType::TargetLocal => self.target_ptr = match_end_position,
            ReferencePointerType::Main => {
                if offset.abs() < SMALL_OFFSET_THRESHOLD {
                    self.main_ref_ptr = match_end_position;
                } else {
                    self.auxiliary_ref_ptr = match_end_position;
                }
            }
            ReferencePointerType::Auxiliary => {
                if offset.abs() < SMALL_OFFSET_THRESHOLD {
                    self.auxiliary_ref_ptr = match_end_position;
                } else {
                    self.main_ref_ptr = match_end_position;
                }
            }
        }
        Ok(())
    }
}

impl Default for MatchPointers {
    fn default() -> Self {
        MatchPointers::new(0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_after_match_should_update_target_ptr_for_target_local_matches() {
        let mut pointers = MatchPointers::new(100, 200, 300);
        let result = pointers.update_after_match(150, -50, ReferencePointerType::TargetLocal);
        assert!(result.is_ok());
        assert_eq!(pointers.target_ptr, 150);
    }

    #[test]
    fn update_after_match_should_return_error_for_position_overflow() {
        let mut pointers = MatchPointers::new(100, 200, 300);
        let result = pointers.update_after_match(usize::MAX, 100, ReferencePointerType::Main);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), PointerError::PositionOverflow);
    }

    #[test]
    fn update_after_match_should_update_main_ptr_for_small_offset() {
        let mut pointers = MatchPointers::new(100, 200, 300);
        pointers.update_after_match(250, SMALL_OFFSET_THRESHOLD - 1, ReferencePointerType::Main).unwrap();
        assert_eq!(pointers.main_ref_ptr, 250);
    }

    #[test]
    fn update_after_match_should_update_auxiliary_ptr_for_large_offset() {
        let mut pointers = MatchPointers::new(100, 200, 300);
        pointers.update_after_match(500, SMALL_OFFSET_THRESHOLD, ReferencePointerType::Main).unwrap();
        assert_eq!(pointers.auxiliary_ref_ptr, 500);
    }

    #[test]
    fn calculate_offset_should_return_target_local_with_negative_offset_when_position_before_target_ptr() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(50);
        assert_eq!(offset, -50);
        assert!(matches!(pointer_type, ReferencePointerType::TargetLocal));
    }

    #[test]
    fn calculate_offset_should_use_main_ref_ptr_when_its_offset_is_smaller() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(210);
        assert_eq!(offset, 10);
        assert!(matches!(pointer_type, ReferencePointerType::Main));
    }

    #[test]
    fn calculate_offset_should_use_auxiliary_ref_ptr_when_its_offset_is_smaller() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(310);
        assert_eq!(offset, 10);
        assert!(matches!(pointer_type, ReferencePointerType::Auxiliary));
    }

    #[test]
    fn calculate_offset_should_prefer_main_ref_ptr_when_offsets_are_equal() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(250);
        assert_eq!(offset, 50);
        assert!(matches!(pointer_type, ReferencePointerType::Main));
    }

    #[test]
    fn calculate_offset_should_handle_position_at_target_ptr_edge_case() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(100);
        assert_eq!(offset, -100);
        assert!(matches!(pointer_type, ReferencePointerType::Main));
    }

    #[test]
    fn calculate_offset_should_handle_position_at_main_ref_ptr_edge_case() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(200);
        assert_eq!(offset, 0);
        assert!(matches!(pointer_type, ReferencePointerType::Main));
    }

    #[test]
    fn calculate_offset_should_handle_position_at_auxiliary_ref_ptr_edge_case() {
        let pointers = MatchPointers::new(100, 200, 300);
        let (offset, pointer_type) = pointers.calculate_offset(300);
        assert_eq!(offset, 0);
        assert!(matches!(pointer_type, ReferencePointerType::Auxiliary));
    }

    #[test]
    fn calculate_offset_should_handle_large_offsets_correctly() {
        let pointers = MatchPointers::new(1000, 2000, 3000);
        let (offset, pointer_type) = pointers.calculate_offset(2500);
        assert_eq!(offset, 500);
        assert!(matches!(pointer_type, ReferencePointerType::Main));
    }
}