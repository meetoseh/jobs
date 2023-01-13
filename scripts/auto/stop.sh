#!/usr/bin/env bash
main() {
    screen -S webapp -X stuff "^C"
    local cnt=0
    while (( $cnt < 15 ))
    do
        if [ -f updater.lock ]
        then
            sleep 1
            cnt=$(($cnt+1))
        else
            break
        fi
    done

    if [ -f updater.lock ]
    then
        echo "Updater is still running, attempting sigquit"
        screen -S webapp -X quit

        sleep 3

        if [ -f updater.lock ]
        then
            echo "Updater is still running, using sigkill"
            kill -9 $(cat updater.lock)
        fi

        rm -f updater.lock
    fi
}

main
