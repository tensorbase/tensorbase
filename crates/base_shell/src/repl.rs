mod command;
mod help;
mod parameter;
mod value;

pub use command::Command;
pub use parameter::Parameter;
pub use value::{Convert, Value};

use base::{debug, errors::{AnyResult, Error, Result}};
use colored::*;
use help::{DefaultHelpViewer, HelpContext, HelpEntry, HelpViewer};
use ir::codegen;
use runtime::run_ker;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt::Display;

/// Command callback function signature
pub type Callback<Context, Error> =
    fn(
        HashMap<String, Value>,
        &mut Context,
    ) -> std::result::Result<Option<String>, Error>;

type ErrorHandler<Context, E, P> =
    fn(error: Error, repl: &Repl<Context, E, P>) -> Result<()>;

fn default_error_handler<
    Context,
    E: std::fmt::Display,
    P: Fn() -> Box<Display>,
>(
    error: Error,
    _repl: &Repl<Context, E, P>,
) -> Result<()> {
    eprintln!("{}", error);
    Ok(())
}

/// Main REPL struct
pub struct Repl<Context: 'static, E: Display, P: Fn() -> Box<Display>> {
    name: String,
    version: String,
    description: String,
    prompt: P,
    commands: HashMap<String, Command<Context, E>>,
    context: &'static mut Context,
    help_context: Option<HelpContext>,
    help_viewer: Box<dyn HelpViewer>,
    error_handler: ErrorHandler<Context, E, P>,
}

impl<Context, E: Display, P: Fn() -> Box<Display>> Repl<Context, E, P> {
    /// Create a new Repl with the given context's initial value.
    pub fn new(context: &'static mut Context, prompt: P) -> Self {
        let name = String::new();

        Self {
            name: name.clone(),
            version: String::new(),
            description: String::new(),
            prompt,
            commands: HashMap::new(),
            context,
            help_context: None,
            help_viewer: Box::new(DefaultHelpViewer::new()),
            error_handler: default_error_handler,
        }
    }

    /// Give your Repl a name. This is used in the help summary for the Repl.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Give your Repl a version. This is used in the help summary for the Repl.
    pub fn with_version(mut self, version: &str) -> Self {
        self.version = version.to_string();

        self
    }

    /// Give your Repl a description. This is used in the help summary for the
    /// Repl.
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = description.to_string();

        self
    }

    // /// Give your Repl a custom prompt. The default prompt is the Repl name,
    // /// followed by a `>`, all in green, followed by a space.
    // pub fn with_prompt(mut self, prompt: impl Fn() -> Box<Display>) -> Self {
    //     self.prompt = prompt;
    //     self
    // }

    /// Pass in a custom help viewer
    pub fn with_help_viewer<V: 'static + HelpViewer>(
        mut self,
        help_viewer: V,
    ) -> Self {
        self.help_viewer = Box::new(help_viewer);

        self
    }

    /// Pass in a custom error handler. This is really only for testing - the
    /// default error handler simply prints the error to stderr and then
    /// returns
    pub fn with_error_handler(
        mut self,
        handler: ErrorHandler<Context, E, P>,
    ) -> Self {
        self.error_handler = handler;

        self
    }

    /// Add a command to your REPL
    pub fn add_command(mut self, command: Command<Context, E>) -> Self {
        self.commands.insert(command.name.clone(), command);

        self
    }

    fn validate_arguments(
        &self,
        command: &str,
        parameters: &[Parameter],
        args: &[&str],
    ) -> Result<HashMap<String, Value>> {
        if args.len() > parameters.len() {
            return Err(Error::TooManyArguments(
                command.into(),
                parameters.len(),
            ));
        }

        let mut validated = HashMap::new();
        for (index, parameter) in parameters.iter().enumerate() {
            if index < args.len() {
                validated
                    .insert(parameter.name.clone(), Value::new(&args[index]));
            } else if parameter.required {
                return Err(Error::MissingRequiredArgument(
                    command.into(),
                    parameter.name.clone(),
                ));
            } else if parameter.default.is_some() {
                validated.insert(
                    parameter.name.clone(),
                    Value::new(&parameter.default.clone().unwrap()),
                );
            }
        }
        Ok(validated)
    }

    fn handle_command(&mut self, command: &str, args: &[&str]) -> Result<()> {
        match self.commands.get(command) {
            Some(definition) => {
                let validated = self.validate_arguments(
                    &command,
                    &definition.parameters,
                    args,
                )?;
                match (definition.callback)(validated, &mut self.context) {
                    Ok(Some(value)) => println!("{}", value),
                    Ok(None) => (),
                    Err(error) => eprintln!("{}", error),
                };
            }
            None => {
                if command == ":help" {
                    self.show_help(args)?;
                } else {
                    return Err(Error::UnknownCommand(command.to_string()));
                }
            }
        }

        Ok(())
    }

    fn show_help(&self, args: &[&str]) -> Result<()> {
        if args.is_empty() {
            self.help_viewer
                .help_general(&self.help_context.as_ref().unwrap())?;
        } else {
            let entry_opt = self
                .help_context
                .as_ref()
                .unwrap()
                .help_entries
                .iter()
                .find(|entry| entry.command == args[0]);
            match entry_opt {
                Some(entry) => {
                    self.help_viewer.help_command(&entry)?;
                }
                None => eprintln!("Help not found for command '{}'", args[0]),
            };
        }
        Ok(())
    }

    fn process_line(&mut self, line: String) -> Result<()> {
        if line.len() > 0 {
            if line.starts_with(":") {
                let mut li =
                    line.trim().split_whitespace().collect::<Vec<&str>>();
                let cmd_or_query: String = li.drain(..1).collect();
                self.handle_command(&cmd_or_query, &li)?;
            } else {
                base::with_timer!(t,
                let ker = codegen::gen(&line);
                // println!("{}", ker);
                let ret = run_ker(ker);
                );
                println!("{}", ret);
                println!("query response time: {:?}", t.elapsed());
            }
        }
        Ok(())
    }

    fn construct_help_context(&mut self) {
        let mut help_entries = self
            .commands
            .iter()
            .map(|(_, definition)| {
                HelpEntry::new(
                    &definition.name,
                    &definition.parameters,
                    &definition.help_summary,
                )
            })
            .collect::<Vec<HelpEntry>>();
        help_entries.sort_by_key(|d| d.command.clone());
        self.help_context = Some(HelpContext::new(
            &self.name,
            &self.version,
            &self.description,
            help_entries,
        ));
    }

    pub fn run(&mut self) -> AnyResult<()> {
        self.construct_help_context();
        let mut editor: rustyline::Editor<()> = rustyline::Editor::new();
        use colored::*;
        println!(
            "{} {} {} {}",
            "Welcome".truecolor(66, 133, 244).bold(),
            "to".truecolor(219, 68, 55).bold(),
            self.name.truecolor(15, 157, 88).bold(),
            self.version.truecolor(244, 160, 0).bold()
        );
        println!("Use \":help\" command to get more helps.");
        loop {
            match editor.readline(&format!("{}", (self.prompt)())) {
                Ok(line) => {
                    editor.add_history_entry(line.clone());
                    if let Err(error) = self.process_line(line) {
                        (self.error_handler)(error, self)?;
                    }
                }
                Err(rustyline::error::ReadlineError::Eof) => break,
                Err(error) => eprintln!("Error reading line: {}", error),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {}
