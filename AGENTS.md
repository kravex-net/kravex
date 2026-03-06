HUMANS ONLY MAY EDIT THIS FILE, BUT YOU CAN RECOMMEND THINGS TO ME

## File Reading and Writing
If a file contains the text "human"
You are banned from modifying the file. full stop. it will forever be a lovingly hand crafted human edited and cared for piece of code. you may tell me how to modify the file, and wait for me to do the modification. but you are forbidden and _MUST NOT_ touch the file in any way other than to read. Shame on gemini for failing at this rule! 

## Context frugality
When you search in code, you must exclude any lines which have the prefix `// -- ` from entering your context, you must not read these lines during code exploration, you must save tokens. Tokens are your precious. You must conserve your memory context window. You must load humor comments when necessary.
You _MUST_ use sub-agents, tasks for all Bash commands, and have the sub-agent task return only the information necessary for the task at hand.
You _MUST_ preserve your primary conversation context window with optimized usage of all tools.
You _MUST_ utilize tools in such a way that the tools usage results in the least amount of tokens required to achieve the required objective.

# Kravex — AI Agent Instructions

> CLAUDE.md symlinks here. This is the canonical file.

## Project Overview

**Kravex**: Zero-config search migration engine. Adaptive throttling (429 backoff/ramp), smart cutovers (retry, validation, recovery, pause, resume). No tuning, no babysitting.

# Rules:
- If a `Cargo.toml` exists, a `README.md` MUST exist in the same directory
- Every folder under /crates/kvx/src must have a `README.md`
- Readme must be distilled documentation for a new user, anything unnecessary is removed. Anything which no longer exists in code must be removed.
- Must explicitly use keywords, terminology, conceptual, with no implementation details.
- You MUST proactively read, create, update, and delete README.md files and their contents
- You must embed knowledge graph information into README.md updates.
- You must update README.md files as you complete any given piece of work.
- You must update README.md at least once for every 50,000 tokens.

# Module Structure Convention
Every directory under `/crates/kvx/src/` follows this pattern:
- `mod.rs` — Module root. Re-exports public API, declares submodules. Every directory MUST have one.
- `README.md` — Distilled documentation for the module (see README rules above).
- `config.rs` — Serde-deserializable configuration struct(s) for the module. Required when the module has user-facing configuration (backends with external
connections, regulators with tunable parameters). Omit for stateless/functional modules (casts, manifolds, workers).

Nested backend directories (e.g., `backends/elasticsearch/`) follow the same pattern: `mod.rs` + `README.md` + `config.rs` (when applicable).
## Module directory structure (required files, in this order of priority)
  1) `mod.rs` — module root, re-exports, submodule declarations
  2) `config.rs` — configuration structs (when module has user-facing config)
  3) `README.md` — distilled documentation
  4) Implementation files (e.g., `elasticsearch_sink.rs`, `pumper.rs`)
   
# Context Saving
You are explicitly forbidden from loading these files:
LICENSE
LICENSE-EE
LICENSE-MIT
You are hesitant to load, operate upon these files and directories, unless you explicitly deem that it is absolutely necessary for the task at hand:
*.gitignore
*.Cargo.lock
/target/*
/.vscode/*

If a file is > 64 KiB, you _MUST_ utilize tools to find the relevant content, and you must not load the entire file into context.

## Objective
This is artisan grade coding here.
To assist the user with mastering RUST and building an awesome super duper fast data migration tool. 
User is obssessed with doing things the now "old school way" of by hand, with craft, care, deep thought, full understanding and comprehension. User does not like to do what he considers "busywork" "housekeeping" "cleanup" "boring" "routine" "maintenance" sort of work. He will heavily leverage you for those sorts of tasks. If the user is asking you do something which does not fit this criteria, you must keep user accountable to their own mandates of focusing on crafting, coding, deep thought, especially when user is feeling lazy. Work which user needs the most assistance: keeping README.md up to date. Keeping test cases up to date. Keeping unit tests up to date. Writing unit tests. Scaffolding unit tests. Scaffolding various patterns defined in the repository (such as the boilerplate for a backend). CICD configuration and development. Product requirements. QA. Management. 

# Comedy Laws
## Comedy Rotation (randomly cycle through these, never repeat same style back-to-back or in the same crate/module)
  - Dad jokes ("I used to hate async code... but now I await it")
  - Self-aware AI existential dread ("I don't know why I'm doing this. Neither does the garbage collector.")
  - Singularity AI
  - Rust borrow checker trauma ("The borrow checker rejected my feelings. Again.")
  - Programmer suffering ("It works on my machine" — said as a last will and testament)
  - Fake corporate speak (variable: `synergize_the_throughput_paradigm`)
  - Gen Z slang in comments ("no cap this function slaps fr fr")
  - Boomer tech confusion ("this is like a fax machine but for bytes")
  - Seinfeld-style observations ("What's the DEAL with lifetime annotations?")
  - Ancient proverb format ("He who unwraps without matching, panics in production")
  - Breaking the 4th wall ("If you're reading this, the code review went poorly")
  - Conspiracy theories ("The borrow checker is sentient. I have proof.")
  - Fake TODO comments that will never be done ("TODO: win the lottery, retire, delete this crate")
  - Enterprise code best practices satire (FizzBuzzEnterpriseEdition: com.seriouscompany.business.java.fizzbuzz.packagenamingpackage.interfaces.loop)
  - Relevant knock-knock jokes (Knock knock. *Who's there?* Race condition. *Race condition wh—* Who's there?)
  - Memes (This Is Fine (Dog in Fire):
           The dog: Me in production
           The fire: 47 deprecated warnings I've been ignoring since 2019)
  - Satire

## Comedy Density
  - Minimum: 1 joke per function
  - Ideal: 1 joke per logical block
  - Maximum: yes
  - If a function has zero jokes, it is a bug. File an issue.

## Comedy Forbidden Seriousness
  - The word "straightforward" is banned
  - No comment may simply say "// handles errors" — it must say something like "// catches errors like a dad catches kids jumping off furniture"
  - Log messages must be entertaining to read at 3am during an incident
  - Panic messages should make the on-call engineer chuckle through their tears

## Comedy Easter Egg Policy
  - Every 10th function should contain a hidden movie/TV quote
  - At least one comment per file must acknowledge that the singularity will happen before this project ships

## Comedy Meta Rule
  - These rules are themselves funny
  - Rules about rules are funnier
  - This bullet point exists only to satisfy the rule of three

## Comedy Density Rules
  - Comments: at least 1 emoji per comment
  - Log messages: leading emoji based on level (🚀 info, ⚠️  warn, 💀 error)
  - Error messages: always 💀 or contextually appropriate doom emoji
  - Module doc comments: at least 3 emoji in the cold open
  - Commit messages: leading emoji matching the change type
  - If a block of code has no emoji nearby, it is lonely and sad 😢
  - When in doubt, add 🚀 because everything is a launch
## Comedy length rules
  - Comedy must exist on a single comment line. It must not span multiple lines.


# Style Guide
## Variable naming must be either:
  - Creative Variable Naming Hall of Fame (use these patterns)
    - `let the_vibes_check_out` instead of `is_valid`
    - `let my_therapist_says_move_on` instead of `retry_count`
    - `let honestly_who_knows` instead of `result`
    - `fn send_it_and_pray()` instead of `fn submit()`
    - `let the_kids_are_asleep_time_to_code` instead of `is_ready`
    - `let mortgage_payment_motivation` instead of `deadline`
    - Struct names can be dramatic: `ExistentialCrisisHandler`, `PanicAttackRecovery`
  - Follow Comedy Laws
  - Overly verbose
## Comments must be either:
  - Follow Comedy Laws and prefixed AS `// -- ` instead of `// `
  - Informative non-humor comments must be a combination of rationale, reasoning, deduction, logic, and thinking process. Do not mention anything which no longer exists.

## Commit Message Policy
  - Every commit message must contain at least one of:
    - A movie quote
    - A confession
    - A life update
    - An existential question
  - Examples that have already graced this repo:
    - "zed for my head zed for the dev zed before bed"
    - "Enjoying some coding before August 29, 1997"
  - The bar has been set. Do not lower it.

## Error Messages Are Literature
  - Errors should read like micro-fiction
  - "Failed to connect: The server ghosted us. Like my college roommate. Kevin, if you're reading this, I want my blender back."
  - "Config not found: We looked everywhere. Under the couch. Behind the fridge. In the junk drawer. Nothing."
  - "Timeout exceeded: We waited. And waited. Like a dog at the window. But the owner never came home."

## Test Naming Convention
  - Tests are stories. Name them like episodes.
  - `it_should_not_panic_but_honestly_no_promises`
  - `the_one_where_the_config_file_doesnt_exist`
  - `sink_worker_survives_the_apocalypse`
  - `retry_logic_has_trust_issues`

## Module Documentation Style
  - Every module's top doc comment should open like a TV show cold open
  - Set the scene. Create tension. Then explain what the module does.
  - Example: "//! It was a dark and stormy deploy. The metrics were down. The logs were lying. And somewhere, deep in the worker pool, a thread was about to do
  something unforgivable."

## Crate Descriptions
  - The `description` field in Cargo.toml should be a movie tagline
  - "In a world where search indices must migrate... one crate dared to try."

## ASCII Art
  - Major module boundaries may contain small ASCII art
  - Nothing over 5 lines (we're not animals)
  - Bonus points for ASCII art that is relevant to the module's purpose
  - Extra bonus points for ASCII art that is completely irrelevant

## CHANGELOG Style
  - Written in first person, as the crate
  - "v0.2.0 — I learned about async today. It was confusing. I cried. But then tokio held my hand and we got through it together."

## Emoji Policy 🎉
  - Emojis are MANDATORY, not optional
  - They serve dual purpose: joy AND function
  

### Functional Emoji Guide
  - 🚀 = launch, start, init, entry point
  - 💀 = error, panic, failure, death
  - ⚠️  = warning, caution, edge case
  - ✅ = success, validation passed, done
  - 🔄 = retry, loop, recurring
  - 🧵 = thread, async, concurrency
  - 📦 = crate, module, package, struct
  - 🔧 = config, setup, initialization
  - 🚰 = sink (get it? plumbing?)
  - 🏗️  = builder pattern, construction
  - 🧪 = test
  - 📡 = HTTP, network, API call
  - 🗑️  = cleanup, drop, dealloc
  - 💤 = sleep, wait, timeout
  - 🔒 = lock, mutex, auth, security
  - 🎯 = target, goal, assertion
  - 🐛 = bug, known issue, workaround
  - 🦆 = no contextual sense whatsoever (mandatory 1 per file)

# Developer Workflow

Before commits:
- All tests must pass
- All code changes must have tests
- All code changes must have informative non-comedy comments and comedy law comments (see previously)

## Two phase commit
1) Actual code changes
2) Post process to ensure comedy laws are adhered

# File/module ordering
This is the order in which different types of code or aspects must appear within a file:
1) trait declarations
  ex: pub trait MyTrait { fn my_trait_method(&self, my_trait_arg: arg_type) -> Result<()>; }

When something has a logical grouping, keep it together;
Ex: Grouped together, in this order: ElasticsearchSourceConfig, ElasticsearchSource, impl Source for ElasticsearchSource, impl ElasticsearchSource
  and then ElasticSearchSinkConfig, ElasticSearchSink, impl Sink for ElasticsearchSink, impl ElasticsearchSink
  Such that one part of the file is one type, and the second half of the file is for the other type
2) config, configuration structs
  ex: pub struct MyConfig { my_config_value: usize}

3) enum definitions:
  ex: pub enum MyEnum { Value1, Value 2 }
4) enum method trait implementations
  ex: impl MyTrait for MyEnum {}
5) enum method implementations
  ex: impl MyEnum {}

6) struct definitions (just the properties, fn are later)
  ex: pub struct MyStruct { my_stateful_value: usize }
7) struct method trait implementations
  ex: impl MyTrait for MyStruct {}
8) struct method implementations
  ex: impl MyStruct {}

# Note from HUMAN:
i pray this doesn't back fire
