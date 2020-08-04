use super::Callback;
use super::Parameter;
use base::errors::*;
use std::fmt;

/// Struct to define a command in the REPL
pub struct Command<Context, E> {
    pub(crate) name: String,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) callback: Callback<Context, E>,
    pub(crate) help_summary: Option<String>,
}

impl<Context, E> fmt::Debug for Command<Context, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Command")
            .field("name", &self.name)
            .field("parameters", &self.parameters)
            .field("help_summary", &self.help_summary)
            .finish()
    }
}

impl<Context, E> std::cmp::PartialEq for Command<Context, E> {
    fn eq(&self, other: &Command<Context, E>) -> bool {
        self.name == other.name
            && self.parameters == other.parameters
            && self.help_summary == other.help_summary
    }
}

impl<Context, E> Command<Context, E> {
    /// Create a new command with the given name and callback function
    pub fn new(name: &str, callback: Callback<Context, E>) -> Self {
        Self {
            name: name.to_string(),
            parameters: vec![],
            callback,
            help_summary: None,
        }
    }

    /// Add a parameter to the command. The order of the parameters is the same
    /// as the order in which this is called for each parameter.
    pub fn with_parameter(
        mut self,
        parameter: Parameter,
    ) -> Result<Command<Context, E>> {
        if parameter.required
            && self.parameters.iter().any(|param| !param.required)
        {
            return Err(Error::IllegalRequiredError(parameter.name));
        }

        self.parameters.push(parameter);

        Ok(self)
    }

    /// Add a help summary for the command
    pub fn with_help(mut self, help: &str) -> Command<Context, E> {
        self.help_summary = Some(help.to_string());

        self
    }
}
