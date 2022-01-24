FROM tdmproject/tdmq-client:master-641

ENV ENVIROPLUS_HOME /opt/enviroplus

WORKDIR $ENVIROPLUS_HOME
COPY requirements.txt "${ENVIROPLUS_HOME}"
RUN pip3 install --no-cache-dir -r requirements.txt
COPY enviroplus.py "${ENVIROPLUS_HOME}"
RUN chmod a+rx "${ENVIROPLUS_HOME}/enviroplus.py"

RUN useradd -m -s /bin/bash -d "${ENVIROPLUS_HOME}" enviroplus
USER enviroplus

ENTRYPOINT ["python3", "./enviroplus.py"]
