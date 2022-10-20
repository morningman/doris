#!/bin/bash
##################################
# This work well on Amazon Linux
##################################
set -ex

agent_download_url='https://doris-pipline-1308700295.cos.ap-hongkong.myqcloud.com/teamcity/buildAgentFull.zip'
server_ip='43.132.222.7'

serverUrl="http://${server_ip}:8111/"

cd ~
echo "Will install teamcity agent in $HOME, sleep 10..." && sleep 10

# download teamcity agent, https://www.jetbrains.com/help/teamcity/install-teamcity-agent.html
wget --continue "$agent_download_url"

mkdir -p teamcity/teamcity-agent
unzip "$(basename $agent_download_url)" -d teamcity/teamcity-agent

# configure agent
cd teamcity/teamcity-agent
cp conf/buildAgent.dist.properties conf/buildAgent.properties
# set serverUrl=xxx
sed -i "s#^serverUrl=.*#serverUrl=$serverUrl#" conf/buildAgent.properties
# set name=xxx
agent_name="$(sudo dmidecode -s system-manufacturer)_$(sudo dmidecode -s system-product-name)_$(hostname -i)"
sed -i "s#^name=.*#name=agent($agent_name)#" conf/buildAgent.properties

# install java
if ! java --version; then
    sudo yum install -y java-11-amazon-corretto.x86_64
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk/"
    export PATH=$JAVA_HOME/bin:$PATH
    echo "
export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:\$PATH
" >>~/.bashrc
    # shellcheck source=/dev/null
    source ~/.bashrc
fi

echo "TeamCity Agent installed in $HOME/teamcity/teamcity-agent "

# start agent
bash bin/agent.sh start

# check if started
if [[ $(ps -ef | grep -c 'teamcity-agent') -gt 1 ]]; then echo 'TeamCity started.'; fi
# if pgrep java; then echo 'TeamCity started.'; fi

echo -e "
Later, authorize this agent '$agent_name' on server, 

'${serverUrl}/agents/unauthorized'
"
