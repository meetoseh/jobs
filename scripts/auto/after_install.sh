#!/usr/bin/env bash
install_python_packages() {
    yum -y install build-essential libssl-dev
    
    if [ ! -d venv ]
    then
        python3 -m venv venv
    fi
    . venv/bin/activate
    python -m pip install -U pip
    pip install -r requirements.txt
    deactivate
}

install_ffmpeg_if_necessary() {
    if command -v ffmpeg >/dev/null 2>&1
    then
        return
    fi

    local OLD_PWD=$(pwd)

    rm -rf /usr/local/src/ffmpeg
    mkdir -p /usr/local/src/ffmpeg
    cd /usr/local/src/ffmpeg
    wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz
    tar -xf ffmpeg-release-arm64-static.tar.xz
    find . -maxdepth 2 -type f -name "ffmpeg" -print -quit | xargs cp -t /usr/bin
    find . -maxdepth 2 -type f -name "ffprobe" -print -quit | xargs cp -t /usr/bin
    cd $OLD_PWD
}

install_cairo_if_necessary() {
    if [ -f /usr/lib64/libcairo.so.2 ]
    then
        return
    fi

    yum -y install cairo
}

install_ffmpeg_if_necessary
install_cairo_if_necessary
install_python_packages
