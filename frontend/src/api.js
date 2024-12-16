import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import * as arrow from 'apache-arrow';

const url = import.meta.env.VITE_API_URL;
const headers = new Headers();
headers.append('Content-Type', 'application/json');
// headers.append('Authorization', 'Bearer ' + import.meta.env.VITE_REACT_APP_API_KEY);

let ddb = null; 
let ddb_conn = null; 

export async function initDDB() {
	try {
		console.log("initDDB")
		if (!('storage' in navigator && 'getDirectory' in navigator.storage)) {
			throw new Error('OPFS is not supported in this browser');
		}

        const MANUAL_BUNDLES= {
            mvp: {
                mainModule: duckdb_wasm,
                mainWorker: mvp_worker,
            },
            eh: {
                mainModule: duckdb_wasm_eh,
                mainWorker: eh_worker,
            },
        };

        // Select a bundle based on browser checks
        const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
        const worker = new Worker(bundle.mainWorker);
        const logger = new duckdb.ConsoleLogger();
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

		const conn = await db.connect();

		/*
			DuckDB has zonemaps and no other indices - except optional adaptive radix tree which is only helpful when selecting less than 0.1% of the rows. 
			So we don't want to replicate the backend metrics table, because filtering on run_uuid will be expensive. 
			Instead we will split our data on run_uuid or metric... Hmmm. 
			When displaying several run_uuids, it would be good to split on metric right. But depends how many run_uuids we have, if we have hundreds it might not be so smart. 
			Anyway. We will start by replicating the metrics table - but with column names like the ChartPicker.js expects them
		*/

		await conn.query(`
			CREATE TABLE IF NOT EXISTS metrics(
				name varchar(32), 
				metric varchar(250), 
				step bigint,
				timestamp bigint, 
				value double
			)
		`);


        ddb = db;
        ddb_conn = conn; 
	} catch (error) {
		console.error('Error initializing persistent DuckDB:', error);
		throw error;
	}
}




const endpoints = {
    experiments: "fe_experiments",
    runs: "fe_runs",
    metrics: "fe_metrics_available",
    data: "fe_metrics"
}

export const HTTP = {

    fetchData: (endpoint, param = "") => {
        console.log("GET: " + endpoint + param); // debugging
        return fetch(url + endpoint + param, { headers })
            .then(response => response.json())
            .then((json) => {
                return json;
            })
            .catch((error) => {
                alert("\n" + error.message + "\n\nCheck browser console for more information. ");
                return [];
            })
    },

    fetchExperiments: async (param = "") => {
        return new Promise((resolve) => {
            HTTP.fetchData(endpoints.experiments, param).then((json) => {
                console.log({fetchExperimentsJson: json})
                let parsed = [];
                json.forEach(data => {
                    parsed.push({
                        "id": data["experiment_id"],
                        "name": data["name"]
                    })
                })
                resolve(parsed);
            });
        });
    },

    fetchRuns: async (param = "") => {
        return new Promise((resolve) => {
            HTTP.fetchData(endpoints.runs, param).then((json) => {
                let parsed = [];
                json.forEach(data => {
                    if (data["workload"] === null) {
                        data["workload"] = "null";
                    }
                    let workloadId = data["experiment_id"] + "-" + data["workload"];
                    parsed.push({
                        "name": data["run_uuid"],
                        "experimentId": data["experiment_id"],
                        "duration": data["duration"],
                        "startTime": data["start_time"],
                        "source": data["data"],
                        "letter": data["letter"],
                        "model": data["model"],
                        "params": data["params"],
                        "status": data["status"],
                        "workload": workloadId
                    })
                })
                resolve(parsed);
            });
        });
    },

    fetchMetrics: async (runs) => {
        if (runs.length > 0) {
            let param = "?run_uuid=in.(";
            runs.forEach(run => {
                param = param + '"' + run.name + '",';
            });
            param = param.substring(0, param.length - 1) + ")";
            return new Promise((resolve) => {
                HTTP.fetchData(endpoints.metrics, param).then((json) => {
                    let uniqueMetrics = [];
                    json.forEach(metric => {
                        const metricIndex = uniqueMetrics.indexOf(metric.key);
                        if (metricIndex === -1) {
                            uniqueMetrics.push(metric.key);
                        }
                    });
                    resolve(uniqueMetrics.sort());
                });
            });
        }
        else {
            return [];
        }
    },

    fetchChart: async (runs, metric, ddb_db_and_conn) => {
        const run_uuids = runs.map(run => run.name); 
        console.log({fetchChart_run_uuids: run_uuids})
        if (!('storage' in navigator && 'getDirectory' in navigator.storage)) {
            console.log("OPFS is not supported!");
            // OLD FUNCTION
            if (runs.length > 0) {
                let param = "?run_uuid=in.(";
                runs.forEach(run => {
                    param = param + '"' + run.name + '",';
                });
                param = param.substring(0, param.length - 1) + ")&key=eq." + encodeURIComponent(metric);
                return new Promise((resolve) => {
                    HTTP.fetchData(endpoints.data, param).then((json) => {
                        let parsed = [];
                        json.forEach(data => {
                            parsed.push({
                                "name": data["run_uuid"],
                                "metric": data["key"],
                                "step": data["step"],
                                "timestamp": data["timestamp"],
                                "value": data["value"],
                            })
                        })
                        resolve(parsed);
                    });
                });
            }
            else {
                console.error("No data found!");
                return [];
            }
        } 
        else if (!!ddb_conn)
        {
            console.time("fetchChart");
            console.log("DuckDB fetchChart!")

            const placeholders = run_uuids.map(() => '?').join(',');
            const maxTimestampsStmt = await ddb_conn.prepare(`
                SELECT name, max(timestamp) as maxTimestamp
                FROM metrics 
                WHERE name IN (${placeholders})
                AND metric = ?
                GROUP BY name
            `);
            const metricsStmt = await ddb_conn.prepare(`
                SELECT *
                FROM metrics 
                WHERE name IN (${placeholders})
                    AND metric = ?
            `);

            const maxTimestampsArrowTable = await maxTimestampsStmt.query(...run_uuids, metric);

            const maxTimestamps = maxTimestampsArrowTable.toArray()
                .map(row => row.toJSON())
                .reduce((acc, row) => {
                    acc[row.name] = row.maxTimestamp;
                    return acc;
                }, {});

            const params = {};
            run_uuids.forEach(run => {
                let param = "?run_uuid=eq." + encodeURIComponent(run);
                
                if (maxTimestamps[run]) {
                    param += "&timestamp=gt." + maxTimestamps[run];
                }

                param += "&key=eq." + encodeURIComponent(metric)
                
                params[run] = param;
            });

            const [backend_results, metrics_in_ddb] = await Promise.all([
                Promise.all(
                    Object.entries(params).map(([run, param]) => 
                        fetch(url + endpoints.data + param)
                            .then(resp => resp.json())
                            .then(data => ({run, data}))
                    )
                ),
                metricsStmt.query(...run_uuids, metric)
                    .then(result => {
                    return result.toArray().map(row => ({
                        ...row.toJSON(),
                        timestamp: Number(row.timestamp),
                        step: Number(row.step)
                    }));
                })
            ]);

            const all_metrics = [...metrics_in_ddb]; 

            const newMetricsToCache = {
                name: [],
                metric: [],
                step: [],
                timestamp: [],
                value: [],
            };

            console.time("parseFetchedData");
            backend_results.forEach(run_data => {
                const resJson = run_data.data; 
                resJson.forEach(data => {
                    all_metrics.push({
                        "name": data["run_uuid"],
                        "metric": data["key"],
                        "step": data["step"],
                        "timestamp": data["timestamp"],
                        "value": data["value"],
                    });
                    newMetricsToCache.name.push(data["run_uuid"]);
                    newMetricsToCache.metric.push(data["key"]);
                    newMetricsToCache.step.push(data["step"]);
                    newMetricsToCache.timestamp.push(data["timestamp"]);
                    newMetricsToCache.value.push(data["value"]);
                });
            });
            console.timeEnd("parseFetchedData");

            console.time("ddb_bulk_insert");

            const arrowTable = arrow.tableFromArrays(newMetricsToCache);
            await ddb_conn.insertArrowTable(arrowTable, {name: 'metrics', create: false});
            const EOS = new Uint8Array([255, 255, 255, 255, 0, 0, 0, 0]);
            await ddb_conn.insertArrowTable(EOS, { name: 'metrics', create: false});

            console.timeEnd("ddb_bulk_insert");



            await maxTimestampsStmt.close();  // Should probably not do this and keep it open throughout and not recreate it, blabla
            await metricsStmt.close();  // Should probably not do this and keep it open throughout and not recreate it, blabla

            console.timeEnd("fetchChart");
            return all_metrics; 
        } 
        else 
        { 
            // Just use OPFS instead of duckdb?? 
            console.time("fetchChart");
            if (runs.length === 0) {
                console.error("No data found!");
                return [];
            }

            const channel = new BroadcastChannel('opfs-lock');
            const lockId = Date.now().toString();
            let hasLock = false;

            channel.onmessage = (event) => {
                if (event.data.type === 'request-lock') {
                    if (!hasLock) {
                        channel.postMessage({ type: 'lock-granted', lockId: event.data.lockId });
                    }
                }
            };

            console.time("lockRequest");
            channel.postMessage({ type: 'request-lock', lockId });

            await Promise.race([
                new Promise(resolve => {
                    const handler = (event) => {
                        if (event.data.type === 'lock-granted' && event.data.lockId === lockId) {
                            // console.log("GOT LOCK from other tab");
                            hasLock = true;
                            channel.removeEventListener('message', handler);
                            resolve();
                        }
                    };
                    channel.addEventListener('message', handler);
                }),
                new Promise(resolve => {
                    // After 100ms, assume we're the only tab and self-grant
                    setTimeout(() => {
                        if (!hasLock) {
                            // console.log("Self-granting lock (timeout)");
                            hasLock = true;
                            resolve();
                        }
                    }, 100);
                })
            ]);

            console.timeEnd("lockRequest");

            let metricsOfAllRuns = []; 
            try {
                const opfsRoot = await navigator.storage.getDirectory(); 
                
                for(let i = 0; i < runs.length; i++) {
                    // console.log(`Trying run ${i+1}/${runs.length}`);
                    let maxTimestamp = 0; 
                    let cachedResponse = []; 

                    let runDir;
                    try {
                        runDir = await opfsRoot.getDirectoryHandle(runs[i].name); // will throw error if doesn't exist
                        // console.log("Got runDir");
                        
                        try {
                            const metricDir = await runDir.getDirectoryHandle(metric); // will throw error if doesn't exist
                            // console.log("Got metricDir");
                            const metricEntries = []; 
                            let largestTimestamp = 0; 
                            let idxOfLargestTimeStampEntry = -1; 
                            let idx = 0; 

                            for await (const [metricEntryName, metricEntryHandle] of metricDir.entries()) {
                                if(metricEntryHandle.kind === 'file') {
                                    // console.log(`Found file in metricDir! ${metricEntryHandle.name}`);
                                    metricEntries.push([metricEntryName, metricEntryHandle]); 
                                    if(largestTimestamp < parseInt(metricEntryName)) {
                                        idxOfLargestTimeStampEntry = idx; 
                                        largestTimestamp = parseInt(metricEntryName); 
                                    }
                                    idx++; 
                                }
                            }
                            if(idx > 1) alert("more than one file in metricdir! Shouldn't happen")

                            if(idxOfLargestTimeStampEntry > -1) {
                                console.time("getCachedFile");
                                const cachedResultFile = await metricEntries[idxOfLargestTimeStampEntry][1].getFile(); 
                                console.timeEnd("getCachedFile");
                                const fileText = await cachedResultFile.text();
                                const fileJson = JSON.parse(fileText);

                                cachedResponse = fileJson.metrics;
                                maxTimestamp = largestTimestamp; 
                            }
                        } catch {
                            // Metric directory doesn't exist yet
                        }
                    } catch {
                        runDir = await opfsRoot.getDirectoryHandle(runs[i].name, { create: true });
                    }

                    let param = "?run_uuid=eq." + runs[i].name; 
                    if(maxTimestamp) {
                        param += "&timestamp=gt." + maxTimestamp;
                    }
                    param += "&key=eq." + encodeURIComponent(metric); 

                    console.time("fetchDataCall");
                    const resJson = await HTTP.fetchData(endpoints.data, param); 
                    console.timeEnd("fetchDataCall");
                    const parsed = []; 
                    let newMaxTimestamp = 0; 
                    
                    console.time("parseFetchedData");
                    resJson.forEach(data => {
                        parsed.push({
                            "name": data["run_uuid"],
                            "metric": data["key"],
                            "step": data["step"],
                            "timestamp": data["timestamp"],
                            "value": data["value"],
                        });
                        if(data["timestamp"] > newMaxTimestamp) {
                            newMaxTimestamp = data["timestamp"]; 
                        }
                    });
                    console.timeEnd("parseFetchedData");

                    const allMetrics = [...cachedResponse, ...parsed]; 

                    metricsOfAllRuns = [...metricsOfAllRuns, ...allMetrics]

                    const metricDir = await runDir.getDirectoryHandle(metric, { create: true });

                    console.time("clearingOPFSDir");
                    for await (const [name, handle] of metricDir.entries()) {
                        if (handle.kind === 'file') {
                            await metricDir.removeEntry(name);
                        }
                    }
                    console.timeEnd("clearingOPFSDir");

                    if(false)
                    {
                        console.time("writingOPFS");
                        // TODO: make truly async !! 
                        const fileHandle = await metricDir.getFileHandle(newMaxTimestamp.toString(), { create: true });
                        const writable = await fileHandle.createWritable();
                        await writable.write(JSON.stringify({
                            metrics : allMetrics
                        }));
                        await writable.close();
                        console.timeEnd("writingOPFS");
                    } else {
                        metricDir.getFileHandle(newMaxTimestamp.toString(), { create: true })
                        .then(fileHandle => fileHandle.createWritable())
                        .then(writable => {
                            return writable.write(JSON.stringify({
                                metrics: allMetrics
                            }))
                            .then(() => writable); // Pass the writable to the next then
                        })
                        .then(writable => writable.close())
                        .catch(error => {
                            console.error('Error writing metrics:', error);
                            throw error; // Re-throw to handle it in the caller if needed
                        });
                    }

                }
            } catch(e) {
                console.log("An error occurred during OPFS"); 
            } finally {
                hasLock = false;
                console.time("releaseLock");
                channel.postMessage({ type: 'release-lock', lockId });
                console.timeEnd("releaseLock");
            }

            console.timeEnd("fetchChart");
            return metricsOfAllRuns; 
        }
    }
}
