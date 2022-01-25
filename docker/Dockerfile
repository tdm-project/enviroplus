FROM tdmproject/tdmq-client:master-641

ENV ENVIROPLUS_HOME /opt/enviroplus

WORKDIR $ENVIROPLUS_HOME

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

USER root

RUN useradd -m -s /bin/bash -d "${ENVIROPLUS_HOME}" enviroplus

COPY enviroplus.py .
COPY enviroplus_entrypoint.sh .

RUN chmod a+rx enviroplus.py enviroplus_entrypoint.sh

RUN chown -R enviroplus .
USER enviroplus

ENTRYPOINT ["./enviroplus_entrypoint.sh"]
