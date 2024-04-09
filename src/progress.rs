use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};

pub(crate) struct ProgressBarBuilder<'a> {
    template: &'a str,
    quiet: bool,
    len: Option<u64>,
}

impl<'a> ProgressBarBuilder<'a> {
    pub(crate) fn new(template: &'a str, quiet: bool) -> Self {
        Self {
            template,
            quiet,
            len: None,
        }
    }

    pub(crate) fn len(mut self, len: u64) -> Self {
        self.len = Some(len);
        self
    }

    pub(crate) fn build(self) -> ProgressBar {
        if self.quiet {
            return ProgressBar::hidden();
        }

        let pbar = if let Some(len) = self.len {
            ProgressBar::new(len)
        } else {
            ProgressBar::new_spinner()
        };

        pbar.with_style(
            ProgressStyle::with_template(self.template).unwrap(),
        )
        .with_finish(ProgressFinish::AbandonWithMessage(
            ", done.".into(),
        ))
    }
}
