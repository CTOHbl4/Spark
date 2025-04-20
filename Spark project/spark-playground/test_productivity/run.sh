#!/bin/bash
max_workers=3
tests_num=2
for (( w=1; w <= $max_workers; ++w ))
do
    max_partitions=2
    for (( p=1; p <= $max_partitions; ++p ))
    do
        max_brokers=2
        for (( b=1; b <= $max_brokers; ++b ))
        do
            for (( t=1; t <= $tests_num; ++t ))
            do
                sleep 1
                echo $w $p $b
                echo NUM_WORKERS=$w >> .env
                echo NUM_PARTITIONS=$p >> .env
                echo NUM_BROKERS=$b >> .env
                docker compose -f docker-compose-spark.yml create
                docker start kafka1 kafka_output spark-master 
                sleep 15
                # workers and consumer connect to spark-master and kafka2 connects to kafka1
                for ((_w=1; _w <= $w; ++_w))
                do
                    docker start spark-worker-$_w
                done
                if [[ $b -eq 2 ]]
                then
                    docker start kafka2
                fi

                # wait for services to start.
                sleep 15
                docker start kafka_ui-input kafka_ui-output
                docker start producer-entertainment producer-fuel producer-taxes producer-shopping
                sleep 20
                docker start python-consumer
                # End checker based on consumer existence.
                # Depending on existence variable is either empty or container id.
                variable=$(docker ps -q -f name="python-consumer")
                while [ -n "$variable" ]
                do
                    sleep 0.3
                    variable=$(docker ps -q -f name="python-consumer")
                done

                # End Spark Session gracefully first.
                docker stop -t 10 spark-master
                for ((_w=1; _w <= $w; ++_w))
                do
                    docker stop -t 10 spark-worker-$_w
                done
                sleep 10
                docker stop -t 5 producer-entertainment producer-fuel producer-taxes producer-shopping
                sleep 5
                docker compose -f docker-compose-spark.yml down
                docker rmi test_productivity-producer-entertainment test_productivity-producer-fuel \
                    test_productivity-producer-shopping test_productivity-producer-taxes \
                    test_productivity-python-consumer test_productivity-spark-master
                rm .env
            done
        done
    done
done
