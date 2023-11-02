#!/bin/bash
cmd=$1

usage() {
    echo "run.sh <command> <arguments>"
    echo "Available commands:"
    echo " register_connector          register a new Kafka connector"
    echo " start_streaming             start streaming to Kafka"
    echo " stop_streaming              stop streaming to Kafka"
    echo "Available arguments:"
    echo " [connector config path]     path to connector config, for command register_connector only"
}

if [[ -z "$cmd" ]]; then
    echo "Missing command"
    usage
    exit 1
fi

case $cmd in
    register_connector)
        if [[ -z "$2" ]]; then
            echo "Missing connector config path"
            usage
            exit 1
        else
            echo "Registering a new connector from $2"
            # Assign a connector config path such as: kafka_connect_jdbc/configs/connect-timescaledb-sink.json
            curl -s -X POST -H 'Content-Type: application/json' --data @$2  http://localhost:8083/connectors
        fi
        ;;
    generate_schemas)
        # Generate data for 1 taxi with number of features in the range from 2 to 10
        python generate_schemas.py --min_features 2 --max_features 10 --num_schemas 1
        ;;
    *)
        echo -n "Unknown command: $cmd"
        usage
        exit 1
        ;;
esac