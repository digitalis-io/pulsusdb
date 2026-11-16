//! Issue #823: `SelectMergeStacktraces` — the merged flame graph — over the
//! Connect protocol, and `GET /api/profiles/v1/merge`.

#[cfg(test)]
mod tests {
    use pulsus_read::profiles::tree;
    use pulsus_write::protocols::profile_model;

    /// D2: the stack read's day slack is the ingest admission window — a
    /// sample is received at most 10 minutes before and 1 hour after its
    /// time.
    #[test]
    fn the_stack_day_slack_is_the_admission_window() {
        assert_eq!(
            tree::STACK_DAY_BEFORE_MS,
            profile_model::REJECT_NEWER_THAN_MS
        );
        assert_eq!(
            tree::STACK_DAY_AFTER_MS,
            profile_model::REJECT_OLDER_THAN_MS
        );
    }
}
