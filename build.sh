#!/bin/bash

cd $(dirname $0)

source_version=4.9.7
current_version=4.9.7-SNAPSHOT
target_file=rocketmq-4.9.7
target_dir=target/build
target_path=${target_dir}/${target_file}

mvn clean package -Dmaven.test.skip=true -T 8

mkdir -p target/build
pushd .
cd target

# wget https://dist.apache.org/repos/dist/release/rocketmq/4.9.7/rocketmq-all-4.9.7-bin-release.zip
##### build from source
git clone https://github.com/apache/rocketmq.git
cd rocketmq
git checkout rocketmq-all-4.9.7
# apply patch
git apply ../../rocketmq.patch

mvn -Prelease-all clean package -DskipTests=true -T 8
cp distribution/target/rocketmq-4.9.7.zip ..
cd ..
unzip ${target_file}.zip -d ./build
popd

# lib
cp rocketmq-proxy-common/target/rocketmq-proxy-common-${current_version}.jar $target_path/lib
cp rocketmq-proxy-namesrv/target/rocketmq-proxy-namesrv-${current_version}.jar $target_path/lib/rocketmq-namesrv-4.9.7.jar
cp rocketmq-proxy/target/rocketmq-proxy-${current_version}.jar $target_path/lib
# resource
cp rocketmq-proxy/src/main/resources/*.xml $target_path/conf
cp rocketmq-proxy/src/main/resources/*.conf $target_path/conf
cp rocketmq-proxy/src/main/resources/mqproxy $target_path/bin
chmod +x $target_path/bin/mqproxy

cd target/build && zip -r ${target_file}-proxy.zip ${target_file}
