# GIC Toolkit (`gic-tk`)

This is some extra tooling for the [PIC-SURE][0] ecosystem being used by the [Genomic Information Commons (GIC)][1] project.

## Caveat Emptor

_Please note this is considered **alpha software**. This repository is very much a work in progress. It's realized vision is still unclear. The goal is to make certain aspects of PIC-SURE maintenance and administration easier. Lots of breaking changes to come. It's currently unaffiliated with [hms-dbmi][8], the primary drivers of PIC-SURE and GIC._

## Quick Start

1. Ensure that you have Java SDK / OpenJDK LTS edition (either version 11, 17 or 21) available on your system path.
2. Download `gic-tk`, or the jar, from the [release page][2]
3. Upon downloading, ensure that it's an executable file, and simply invoke

```bash
gic-tk
```

or

```bash
java =jar gic-tools-0.1.51-standalone.jar
```

By default it will show you the available commands to invoke.  You should see something like so:

```
Usage: gic-tk <subcommand> <options>

     Most subcommands support the options:
       --help   subcommand help
       --debug  enable debug logging mode

     Subcommands:

     irdb: commands to manipulate Intermediate Representation Databases (irdb)
       init     initialize a new irdb
       add      add or update concept data to an existing irdb
       merge    merge multiple target irdbs into a source irdb
       dump     generate a javabin file from an irdb
       inspect  inspect an irdb contents
       help     subcommand help message

     help: this help message
     version: version number information
```

## Architecture

`gic-tk` is designed as a command-line application that consists of various subcommands to deal with various aspects of PIC-SURE maintenance and administration.  Currnently, there's one subcommand `irdb` _(see below)_.

## Usage

### `irdb` subcommand

"IRDB" is an acronym for "Intermediate Representation Database". The PIC-SURE system loads its phenotypic, or Electronic Health Record (EHR), data via binary "javabin" files on the file system. These javabin files are essentially an organized concatenation of serialized java data structures. The IRDB subcommand aims to make the assembly, creation, and inspection of these PIC-SURE javabin storage files more practical and accessible.

The current PIC-SURE tooling constructs these javabin files from "compiling" a source of CSV files.  The compilation process is currently an "all or nothing" process. While this is effective for small data sets, it becomes unwieldy as the phenotypic data grows both in terms of quantity and variety.  This group of subcommands provides a way to manage and create the javabin binary files from the source CSV (or parquet) files through a set of intermediate representation (IR) files that are based on [DuckDB][3].  These IRDB files can be created in parallel, have data added to it arbitrarily, merged together to consolidate information, and of course, generate associated javabin files.

#### `irdb init`

Create an empty IRDB file.

```
$ gic-tk irdb help init
Usage: gic-tk irdb init <options> <arguments>
Options:
      --debug                   false Enable additional debug logging
  -i, --dbpath /path/to/irdb.db       The file path to the irdb database [required]
```

#### `irdb add`

Add phenotypic/EHR data to an existing IRDB file.

```
$ gic-tk irdb help add
Usage: gic-tk irdb add <options> <arguments>
Options:
      --debug                                false  Enable additional debug logging
  -i, --input-parquet /path/to/input.parquet        The input data source to add to the irdb database [required]
  -o, --target-irdb   /path/to/irdb.db              The target irdb database to add data into [required]
  -c, --concept       \concept\path                 A specific concept path to add into the irdb from the input parquet data source
  -l, --concepts-list /path/to/concepts.list        A list of concept paths to add into the irdb from the input parquet data source (one concept per line)
      --interval      INTEGER                100000 The interval to display record processing updates
```

_**Note**:_ If the input irdb file already contains the concept path of interest, it will _append_ the observations from the source file into existing database concept record (aka cube).

#### `irdb merge`

Merge multiple IRDB's into a single IRDB.

```
$ gic-tk irdb help merge
Usage: gic-tk irdb merge <options> /path/to/irdb1.db /path/to/irdb2.db ...
Options:
      --debug                           false Enable additional debug logging
  -m, --main-irdb /path/to/main-irdb.db       The main input irdb database to merge cubes into [required]
```

_**Note**:_ If the main irdb file already contains the concept path of interest, it's concept record (aka cube) will be _overwritten_ by the concept record contained in the child irdb file(s) being merged.  The last child irdb specified in the command line with the concept record "wins".

#### `irdb dump`

Dump javabin files from a given input IRDB.

```
$ gic-tk irdb help dump
Usage: gic-tk irdb dump <options> <arguments>
Options:
      --debug                                   false Enable additional debug logging
  -i, --input-irdb      /path/to/input.parquet        The input data source to add to the irdb database [required]
  -t, --target-dir      /path/to/javabin.store/       The target javabin directory to create and place data into [required]
  -e, --encryption-file /path/to/encryption_key       The encryption key file to secure the observation store files [required]
```

#### `irdb inspect`

Inspect the contents of an IRDB file (both metadata and raw observation records).

```
$ gic-tk irdb help inspect

Usage: gic-tk irdb inspect <options> <arguments>
Options:
      --debug                                   false Enable additional debug logging
  -i, --irdb             /path/to/irdb.db             The irdb database to inspect [required]
  -c, --concept          \concept\path                A specific concept path to add into the irdb from the input parquet data source
  -l, --concepts-list    /path/to/concepts.list       A list of concept paths to add into the irdb from the input parquet data source (one concept per line)
      --show-data                               false Display the raw observation data
      --display-concepts                        false Display the list of concepts paths in the irdb
      --limit            INTEGER                      limit the number of observations to show when displaying raw observation data
```

#### Example Use Cases

```
# initialize irdb databases
gic-tk irdb init -i age.duckdb
gic-tk irdb init -i race.duckdb
gic-tk irdb init -i age-race-merged.duckdb

# generate "age" and "race" irdb databases independently
gic-tk irdb add --input-parquet age.parquet --target-irdb age.duckdb
gic-tk irdb add --input-parquet race.parquet --target-irdb race.duckdb

# merge the "age" and "race" irdb databases into one
gic-tk irdb merge --main-irdb age-race-merged.duckdb age.duckdb race.duckdb

# create the javabin files from the merged irdb database
mkdir -p javabin-out
gic-tk irdb dump -i age-race-merged.duckdb -t javabin-out -e encryption_key

# inspect the overall summary contents of the merged irdb database
gic-tk irdb inspect --irdb age-race-merged.duckdb

# inspect the summary contents for just a single age concept
gic-tk irdb inspect -i age-race-merged.duckdb -c '\ACT Demographics\Age\'

# view the first 10 observation records for a specific age concept
gic-tk irdb inspect -i age-race-merged.duckdb -c '\ACT Demographics\Age\' --show-data --limit 10

# view all the concept paths currently in the irdb database
gic-tk irdb inspect -i age-race-merged.duckdb --display-concepts
```

## Development

The toolkit is currently written in [clojure][6].  Please have the following requirements installed and available to tinker with `gic-tk`:

1. Java SDK / OpenJDK LTS edition (either version 11, 17 or 21) -- see https://adoptium.net if you need to download a SDK
2. [Clojure CLI Tools][7]
3. GNU or BSD Make

### Custom Building PIC-SURE

This code is built upon a forked repository of [pic-sure-hpds][4].  See [build-pic-sure.sh][5] for more details.

### Helpful Commands

#### Create an uberjar _(and executable jar)_

```
make uberjar
```

#### To run a command straight from the current source code tree

Assuming you're in the root directory of the repository:

```
clj -M:cli irdb help
```

## Contribution

This utility is open to contribution. Feel free to open issues or submit PRs.

## License

ISC

[0]: https://github.com/hms-dbmi/pic-sure
[1]: https://www.genomicinformationcommons.org/
[2]: https://github.com/i2-wustl/gic-toolkit/releases
[3]: https://duckdb.org
[4]: https://github.com/i2-wustl/pic-sure-hpds/tree/wustl-phenotype-etl-adjustments
[5]: https://github.com/i2-wustl/pic-sure-hpds/blob/wustl-phenotype-etl-adjustments/scripts/build-pic-sure.sh
[6]: https://clojure.org
[7]: https://clojure.org/guides/install_clojure
[8]: https://dbmi.hms.harvard.edu
