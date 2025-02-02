# PIC-SURE Extra

This is some extra tooling for the [PIC-SURE][0] ecosystem being used by the [Genomic Information Commons (GIC)][1] project.

This repository is very much a work in progress. It's realized vision is still unclear. The goal is to make certain aspects of PIC-SURE maintainence and administration easier. Lots of breaking changes to come.

The toolkit is currently written in [clojure][6].  Please have the following requirements installed and available:

1. Java SDK / OpenJDK LTS edition (either version 11, 17 or 21) -- see https://adoptium.net if you need to download a SDK
2. [Clojure CLI Tools][7]
3. GNU or BSD Make

## Commands

### Create an uberjar

```
make uberjar
```

### Create Intermediate (IR) Databases (IRDB): `create-irdb`

#### Test Out Command

```bash
make irdb
```

or

```
java -Xmx6g -jar ${JAR} create-irdb \
    --sqlite ${SQLITE_DB_PATH} \
    --column-meta ${COLUMN_META_PATH} \
    --observation-store ${OBSERVATION_STORE_PATH} \
    --encryption-key ${ENCRYPTION_KEY_PATH} \
    --batch-number ${BATCH_NUMBER}
```

See [create-irdb-batch-run.sh][2] and [lsf-batch-create-irdb-submit.sh][3] for invocation on [WUSTL RIS'][4] scientific compute platorm.

### Merge IRDBs

See [create-master-phenocube-db.sh][5] for merging sqlite Intermediate IRDB shards into a master sqlite database.

### Create javabin from Master IRDB and append extra data: `mungeit`

```bash
make mungeit
```

or 

```bash
java -jar ${JAR} mungeit \
    --duckdb ${DUCKDB_PATH} \
    --encryption-key  ${ENCRYPTION_KEY_PATH} \
    --new-store-dir ${NEW_OBSERVATION_STORE_PATH} \
    --column-meta ${INPUT_COLUMNMETA_PATH} \
    --observation-store ${INPUT_OVBSERVATION_STORE_PATH} \
    --irdb ${MASTER_IRDB_PATH}
```

See [lsf-batch-submit.sh][9] and [runit.sh][10] for invocation on [WUSTL RIS'][4] scientific compute platform.


### Clean Up the Build Directory

```
make clean
```

## Current Overall Project Directory Structure

```
Root Dir: /scratch1/fs1/gic/idas/etl

./logs
./m2
./data
./data/output
./data/output/2024-05-19
./data/input
./data/input/2024-01-13
./git
./git/pic-sure
./git/pic-sure-hpds
./git/pic-sure-extras
./java-env
```

## NOTES

These are more notes for my future self [(indraniel)][8], or a select few others. This branch isn't meant for general usage.

_tbh, i'm not sure on the future of this branch.  If we continue, there will definitely be things to cherry-pick from this branch though!_

[0]: https://github.com/hms-dbmi/pic-sure
[1]: https://www.genomicinformationcommons.org/
[2]: ./scripts/create-irdb-batch-run.sh
[3]: ./scripts/lsf-batch-create-irdb-submit.sh
[4]: https://ris.wustl.edu
[5]: ./scripts/create-master-phenocube-db.sh
[6]: https://clojure.org
[7]: https://clojure.org/guides/install_clojure
[8]: https://github.com/indraniel
[9]: ./scripts/lsf-batch-submit.sh
[10]: ./scripts/runit.sh
