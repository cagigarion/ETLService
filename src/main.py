import argparse
import json
import logging
import logging.config
from module_loader.plugin_loader import PluginLoader
import os.path
import schedule
import time
import pandas as pd

# logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)
enviroment_config = "./config"
def job():
    print("I'm working...")



def set_inteval_path(path_interval):
    logging.info("set interval config:"+path_interval)
    with open(enviroment_config, 'w') as interval_config:
        interval_config.write(json.dumps({"interval_path": path_interval}))


def get_inteval_path():
    with open(enviroment_config) as flog:
        obj = json.loads(flog.read())
        return obj.get("interval_path")


schedule.every(3).seconds.do(job)


def get_run_next_time():
    try:
        interval_config_path = get_inteval_path()
        logging.info("starting function get_run_next_time")
        logging.info("interval_config_path:" + interval_config_path)
        with open(interval_config_path) as flog:
            obj = json.loads(flog.read())
            run_next_time = obj.get("run_next_time")
        if run_next_time is None:
            return
        return pd.to_datetime(run_next_time)
    except Exception as ex:
        logging.error(f'Problem when parser the run_next_time {run_next_time}')
        logging.error(f'{ex}')


def set_run_next_time(next_time):
    interval_config_path = get_inteval_path()
    logging.info("starting function set_run_next_time")
    logging.info("interval_config_path:" + interval_config_path)
    str_next_time = None
    if next_time is not None:
        str_next_time = next_time.strftime("%Y%m%d-%H:%M:%S.%f")

    json_dumps = json.dumps({"run_next_time": str_next_time})
    logging.info("set next time value: "+json_dumps)
    with open(interval_config_path, "w") as config_file:
        config_file.write(json_dumps)


def etl_process(args):
    print(vars(args))
    # print(type(args))
    # print(vars(args))

    dt_conf_file = args.conf_file
    # print(type(dt_conf_file))

    try:
        with open(dt_conf_file, "r") as conf_file:
            dt_config = json.load(conf_file)
    except Exception as ex:
        logging.error(f'Problem when handling the input file {dt_conf_file}')
        logging.error(f'{ex}')
        exit(0)

    dt_conf_log_file = args.conf_file_log
    if (os.path.exists(dt_conf_log_file)):
        logging.config.fileConfig(
            dt_conf_log_file, disable_existing_loggers=False)

    # override interval config:    
    set_inteval_path(args.conf_interval_file)
    set_run_next_time(None)

    # create logger
    logger = logging.getLogger('simpleExample')
    '''
    Prepare Operations by having processors ready for applying to many messages
    '''
    logging.debug(f'Info Config {dt_config}')

    # try call schedule process

    sink_to_des = dt_config["destinations"]
    sink_plugins = []
    for des in sink_to_des:
        try:
            sink_plugin = PluginLoader().get_sink_by_name(des["type"])
            if (sink_plugin != None):
                sink_plugin.config(**des)
                sink_plugin.initial_connection()
                sink_plugins.append(sink_plugin)
        except Exception as e:
            logging.error(
                f'Have not found the sink plugin {des["type"]} yet. {e}')

    data_source_type = dt_config["source"]["type"]
    data_source_function = PluginLoader().get_source_by_name(data_source_type)
    if (data_source_function != None):
        data_source_function.set_sink_plugins(sink_plugins)
        data_source_function.handler(**dt_config)
    else:
        print(f"Have not support this data type [{data_source_type}] yet")


def scheduke_process(args):
    print('process job')

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    print('process ETL')

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--conf_file', help='configuration file')
    parser.add_argument('-l', '--conf_file_log', help='configuration log file')
    parser.add_argument('-i', '--conf_interval_file',
                        help='configuration interval file')

    args = parser.parse_args()
    etl_process(args)
    # t_etl = threading.Thread(target=etl_process, args=(args,))
    # t_scheduler = threading.Thread(target=scheduke_process, args=(args,))

    # t_etl.start()
    # t_scheduler.start()

    # t_etl.join()
    # t_scheduler.join()
