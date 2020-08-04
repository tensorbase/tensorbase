use super::Parameter;
use base::errors::*;
use colored::*;
/// Help entry which gets sent to [HelpViewer](trait.HelpViewer.html) when help
/// for a particular command is requested
#[derive(Debug)]
pub struct HelpEntry {
    /// Command from `help <command>`
    pub command: String,

    /// Parameters defined for the command
    pub parameters: Vec<(String, bool)>,

    /// Help summary for the command
    pub summary: Option<String>,
}

impl HelpEntry {
    pub(crate) fn new(
        command_name: &str,
        parameters: &[Parameter],
        summary: &Option<String>,
    ) -> Self {
        Self {
            command: command_name.to_string(),
            parameters: parameters
                .iter()
                .map(|pd| (pd.name.clone(), pd.required))
                .collect(),
            summary: summary.clone(),
        }
    }
}

/// Struct which gets sent to [HelpViewer](trait.HelpViewer.html) when `help`
/// command is called
pub struct HelpContext {
    /// Application name
    pub app_name: String,

    /// Application version
    pub app_version: String,

    /// Application purpose/description
    pub app_purpose: String,

    /// List of help entries
    pub help_entries: Vec<HelpEntry>,
}

impl HelpContext {
    pub(crate) fn new(
        app_name: &str,
        app_version: &str,
        app_purpose: &str,
        help_entries: Vec<HelpEntry>,
    ) -> Self {
        Self {
            app_name: app_name.into(),
            app_version: app_version.into(),
            app_purpose: app_purpose.into(),
            help_entries,
        }
    }
}

/// Trait to be used if you want your own custom Help output
pub trait HelpViewer {
    /// Called when the plain `help` command is called with no arguments
    fn help_general(&self, context: &HelpContext) -> Result<()>;

    /// Called when the `help` command is called with a command argument (i.e.,
    /// `help foo`). Note that you won't have to handle an unknown command -
    /// it'll be handled in the caller
    fn help_command(&self, entry: &HelpEntry) -> Result<()>;
}

/// Default [HelpViewer](trait.HelpViewer.html)
pub struct DefaultHelpViewer;

impl DefaultHelpViewer {
    pub fn new() -> Self {
        Self
    }
}

impl HelpViewer for DefaultHelpViewer {
    fn help_general(&self, context: &HelpContext) -> Result<()> {
        self.print_help_header(context);
        for entry in &context.help_entries {
            print!("{}", entry.command);
            if entry.summary.is_some() {
                print!(" - {}", entry.summary.clone().unwrap());
            }
            println!();
        }

        Ok(())
    }

    fn help_command(&self, entry: &HelpEntry) -> Result<()> {
        if entry.summary.is_some() {
            println!("{}: {}", entry.command, entry.summary.clone().unwrap());
        } else {
            println!("{}:", entry.command);
        }
        println!("Usage:");
        print!("\t{}", entry.command);
        for param in entry.parameters.clone() {
            if param.1 {
                print!(" {}", param.0);
            } else {
                print!(" [{}]", param.0);
            }
        }

        Ok(())
    }
}

impl DefaultHelpViewer {
    fn print_help_header(&self, context: &HelpContext) {
        let header = format!(
            "{} {}: {}",
            context.app_name, context.app_version, context.app_purpose
        );
        let underline = std::iter::repeat(" ")
            .take(header.len())
            .collect::<String>()
            .underline();
        println!("{}", header);
        println!("{}", underline);
    }
}
