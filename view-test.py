import json
import time

from TM1py import TM1Service
from TM1py.Objects import Process
import configparser
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor, wait
import pandas as pd


def run_query(tm1):
    start_process_time = time.perf_counter()
    mdx = '''
    SELECT {DRILLDOWNMEMBER({[ReadTest Dimension 4].[ReadTest Dimension 4].[Total ReadTest Dimension 4]}, 
    {[ReadTest Dimension 4].[ReadTest Dimension 4].[Total ReadTest Dimension 4]})} ON 0, 
    {DRILLDOWNMEMBER({[ReadTest Dimension 3].[ReadTest Dimension 3].[Total ReadTest Dimension 3]}, 
    {[ReadTest Dimension 3].[ReadTest Dimension 3].[Total ReadTest Dimension 3]})} ON 1 
    FROM [ReadTest] 
    WHERE ([ReadTest Dimension 1].[ReadTest Dimension 1].[Total ReadTest Dimension 1], 
    [ReadTest Dimension 2].[ReadTest Dimension 2].[Total ReadTest Dimension 2])
    '''
    try:
        cells = tm1.cells.execute_mdx_cellcount(mdx)

        end_process_time = time.perf_counter()
        elapsed_process_time = end_process_time - start_process_time
        return elapsed_process_time
    except Exception as e:
        return e


async def run_multiple_query(number_of_tasks, tm1):
    with ThreadPoolExecutor(max_workers=number_of_tasks) as exec:
        result_list = []
        futures = [exec.submit(run_query, tm1) for i in range(number_of_tasks)]
        wait(futures, 1000, 'ALL_COMPLETED')
        for future in futures:
            result_list.append(future.result())
    return result_list

if __name__ == "__main__":

    test_cases = [1, 5, 10, 20, 40, 80, 160, 320]
    replica_cases = [0, 1, 2, 3, 4]

    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))

    with TM1Service(**config['tm1srv02']) as tm1:

        print(tm1.connection.session_id)
        print(tm1.cubes.get_model_cubes())

        md = tm1.connection.get_api_metadata()

        results = {'Queries': [], 'Replicas': [], "Time": [], "Partial": [], "Failed": []}
        replica = 0

        for case in test_cases:
            #wait 2 seconds to start test
            time.sleep(2)

            #initialize counters
            failed = 0
            start = time.perf_counter()

            #run queries
            runs = asyncio.run(run_multiple_query(case, tm1))
            elapsed = time.perf_counter() - start

            #check results
            for run in runs:
                partial = False
                if not isinstance(run, float):
                    partial = True
                    failed += 1

            results['Queries'].append(case)
            results['Replicas'].append(replica)
            results['Time'].append(elapsed)
            results['Partial'].append(partial)
            results['Failed'].append(failed)
            print(results)
        df = pd.DataFrame.from_dict(results)
        print(df)
        df.to_csv(f"./results-{time.time()}")
