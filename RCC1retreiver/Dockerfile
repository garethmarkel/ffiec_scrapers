FROM amazonlinux
RUN yum update -y
RUN yum install -y \
    gcc \
    openssl-devel \
    zlib-devel \
    libffi-devel \
    python3 \
    python3-pip \
    git \
    Xvfb \
    gtk3 \
    dbus-glib \
    wget && \
    yum -y clean all
RUN yum -y groupinstall development
WORKDIR /opt

RUN wget -O- "https://download.mozilla.org/?product=firefox-latest-ssl&os=linux64&lang=en-US" | tar -jx -C /usr/local/
RUN ln -s /usr/local/firefox/firefox /usr/bin/firefox

RUN pip3 install --no-cache-dir selenium boto3 xvfbwrapper mysql-connector-python pandas


RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz
RUN tar -xf geckodriver-v0.30.0-linux64.tar.gz
RUN ls -lta
RUN rm geckodriver-v0.30.0-linux64.tar.gz

RUN chmod +x geckodriver
RUN export DISPLAY=:99
#RUN Xvfb -ac -nolisten inet6 :99 &


COPY run.sh /opt/run.sh
COPY RCC1Retreiver.py /opt/RCC1Retreiver.py 
COPY utils/* /opt/utils/
RUN mkdir /opt/downloads/ 
RUN chmod +x /opt/run.sh
ENTRYPOINT ["/opt/run.sh", "--no-save"] 
