#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export M2_HOME=${SCRIPT_DIR}/../m2
export MAVEN_OPTS="-Dmaven.repo.local=${M2_HOME}"
BUILD_DIR=${SCRIPT_DIR}/../tmp-build-pic-sure

function log {
    local timestamp=$(date +"%Y-%m-%d %T")
    echo "---> [ ${timestamp} ] $@" >&2
}

function ensure_build_dir {
    if [ ! -d ${BUILD_DIR} ]; then
        log "Creating temporary PIC-SURE build directory: ${BUILD_DIR}"
        mkdir -p ${BUILD_DIR}
    fi
}

function build_pic_sure {
    log "Building PIC-SURE"
    log "Entering into build directory: ${BUILD_DIR}"
    pushd ${BUILD_DIR}
    log "Cloning hms-dbmi/pic-sure"
    git clone git@github.com:hms-dbmi/pic-sure pic-sure
    log "Entering into pic-sure git repository"
    cd pic-sure
    log "running maven"
    mvn clean install -DskipTests -Dwildfly.skip=true
    popd
}

function build_pic_sure_hpds {
    log "Building PIC-SURE-HPDS"
    log "Entering into build directory: ${BUILD_DIR}"
    pushd ${BUILD_DIR}
    log "Cloning indraniel/pic-sure-hpds"
    git clone git@github.com:i2-wustl/pic-sure-hpds pic-sure-hpds
    log "Entering into pic-sure-hpds git repository"
    cd pic-sure-hpds
    log "Entering into the 2025-loading-idea branch"
    git checkout -b 2025-loading-idea origin/2025-loading-idea
    log "Suppress war module building"
    awk '/<module>war<\/module>/{next} {print}' pom.xml > pom.xml.new && mv pom.xml.new pom.xml
    log "running maven"
    mvn clean install -DskipTests -Dwildfly.skip=true
    popd
}

function cleanup {
    log "remove pic-sure-hpds git repository directory"
    rm -rf ${BUILD_DIR}/pic-sure-hpds
    log "remove pi-sure git repository directory"
    rm -rf ${BUILD_DIR}/pic-sure-hpds
    log "remove temporary build directory: ${BUILD_DIR}"
    rm -rf ${BUILD_DIR}
}

ensure_build_dir ;
build_pic_sure ;
build_pic_sure_hpds ;
cleanup ;
