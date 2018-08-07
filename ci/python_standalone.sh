unset STREAMS_DOMAIN_ID
unset STREAMS_INSTANCE_ID
unset VCAP_SERVICES
unset STREAMING_ANALYTICS_SERVICE_NAME

${PYTHONHOME}/bin/python -m pip install $WORKSPACE

cd $WORKSPACE
pyv=`$PYTHONHOME/bin/python -c 'import sys; print(str(sys.version_info.major)+str(sys.version_info.minor))'`
now=`date +%Y%m%d%H%M%S`
xuf="nose_runs/TEST-PY${pyv}_${now}.xml"

wd="nose_runs/py${pyv}"
mkdir -p ${wd}
nosetests --where=${wd} --xunit-file ${xuf} --config=nose.cfg ../../streamsx/standard/tests
