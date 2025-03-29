#!/usr/bin/env bash -e

APP_ENV=${1-"prod-jpw1"}
DATE_HOUR=${2-`date -u +"%Y-%m-%dT%H"`}
FORCE_DELETE_PATH=${3-true}
ENV_FILE=application-$APP_ENV.conf    # application-prod-{env}.conf
PROJECT_DIR=/home/ratuser/$APP_ENV/metastore

echo "Date Hour: $DATE_HOUR"
echo "Environment file: $ENV_FILE"
echo "Snapshot file (service_group): $SG_FILE"
echo "Snapshot file (service_category): $SC_FILE"

# =================================
# Variables
# ==================================
HADOOP_BASH="source ~/.bashrc"
HADOOP_CLASSPATH=`${HADOOP_BASH} && hadoop classpath`
KRB5_CONF=~/hdp_c6000/krb5.conf
METASTORE_JAR=$PROJECT_DIR/target/scala-2.12/metastore-assembly.jar

echo "=================================="
echo "Running command:"
echo "=================================="
CMD="${HADOOP_BASH} && java \
-cp ${MOKSHA_JAR}:${HADOOP_CLASSPATH} \
-Dapp.conf=$PROJECT_DIR/src/main/resources/$ENV_FILE \
-Djava.security.krb5.conf=${KRB5_CONF} \
-Xmx128m \
com.metastore.moksha.query.SGCTranscriber \
--executionDateHour=$DATE_HOUR \
--forceDeleteOutputPath=$FORCE_DELETE_PATH"
echo ${CMD}

${HADOOP_BASH} && java \
-cp ${METASTORE_JAR}:${HADOOP_CLASSPATH} \
-Dapp.conf=$PROJECT_DIR/src/main/resources/$ENV_FILE \
-Djava.security.krb5.conf=${KRB5_CONF} \
-Xmx128m \
com.metastore.moksha.query.SGCTranscriber \
--executionDateHour=$DATE_HOUR \
--forceDeleteOutputPath=$FORCE_DELETE_PATH