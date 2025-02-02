.PHONY: repl clean uberjar

repl:
	clj -M:repl

pic-sure:
	bash ./scripts/build-pic-sure.sh

mungeit:
	clj -M:cli mungeit \
		--duckdb /scratch1/fs1/gic/idas/etl/data/input/duckdbs/2024-06-03-genomics.duckdb \
		--encryption-key  /scratch1/fs1/gic/idas/etl/data/input/encryption_key \
		--new-store-dir /scratch1/fs1/gic/idas/etl/data/output/2024-06-05 \
		--column-meta /scratch1/fs1/gic/idas/etl/data/input/2024-01-13/columnMeta.javabin \
		--observation-store /scratch1/fs1/gic/idas/etl/data/input/2024-01-13/allObservationsStore.javabin \
		--irdb /scratch1/fs1/gic/idas/etl/data/output/create-irdb/2024-01-13-master.sqlite.db

irdb-init:
	clj -M:cli create-irdb \
		--sqlite ./data/1-irdb.sqlite \
		--column-meta ./data/2024-01-13/columnMeta.javabin \
		--observation-store ./data/2024-01-13/allObservationsStore.javabin \
		--encryption-key ./data/2024-01-13/encryption_key \
		--batch-number 1

clean:
	clj -T:build clean

uberjar:
	clj -T:build uber
