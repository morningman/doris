#!/bin/bash
###########################################
# This work well on [Amazon Linux, CentOS]
###########################################
set -ex

agent_download_url='https://doris-pipline-1308700295.cos.ap-hongkong.myqcloud.com/teamcity/buildAgentFull.zip'
server_ip='43.132.222.7'
java_version='11'
agent_count_to_install=1

serverUrl="http://${server_ip}:8111/"

cd ~
echo "Will install $agent_count_to_install teamcity agent in $HOME/teamcity, sleep 10..." && sleep 10

# install java on Amazon Linux
if ! java --version; then
    if sudo yum install -y java-11-amazon-corretto.x86_64; then
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk/"
        export PATH=$JAVA_HOME/bin:$PATH

        echo "
export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:\$PATH
" >>~/.bashrc

        set +x
        # shellcheck source=/dev/null
        source ~/.bashrc
        set -x
    fi
fi

# install java on CentOS
if ! java --version; then
    if [[ "$java_version" == "11" ]]; then
        # java 11
        sudo yum install -y java-11-openjdk."$(uname -m)" java-11-openjdk-devel."$(uname -m)"
        java_dir=$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-11-*' | sed -n '1p')
    elif [[ "$java_version" == "8" ]]; then
        # java 8
        sudo yum install -y java-1.8.0-openjdk."$(uname -m)" java-1.8.0-openjdk-devel."$(uname -m)"
        java_dir=$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-1.8.0-*' | sed -n '1p')
    else
        echo "java version only in [8, 11]"
        exit 1
    fi

    echo "
export JAVA_HOME=$java_dir
export PATH=\$JAVA_HOME/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

java --version

# download teamcity agent, https://www.jetbrains.com/help/teamcity/install-teamcity-agent.html
if ! wget --continue "$agent_download_url"; then echo "download failed!!" && exit 1; fi

for ((i = 0; i < agent_count_to_install; ++i)); do
    teamcity_agent_home="$HOME/teamcity/teamcity-agent-$i"
    if [[ -d "$teamcity_agent_home" ]]; then echo "$teamcity_agent_home existed, please remove it firstly..." && exit 1; fi
    mkdir -p "$teamcity_agent_home"
    unzip "$(basename $agent_download_url)" -d "$teamcity_agent_home"

    # configure agent
    cd "$teamcity_agent_home"
    cp conf/buildAgent.dist.properties conf/buildAgent.properties
    # set serverUrl=xxx
    sed -i "s#^serverUrl=.*#serverUrl=$serverUrl#" conf/buildAgent.properties
    # set name=xxx
    agent_name="$(sudo dmidecode -s system-manufacturer)_$(sudo dmidecode -s system-product-name)_$(hostname)"
    sed -i "s#^name=.*#name=agent($agent_name)#" conf/buildAgent.properties
    cd -

    echo "TeamCity Agent installed in $teamcity_agent_home "

    # start agent
    if ! bash "$teamcity_agent_home/bin/agent.sh" start; then echo 'Start TeamCity Agent fail...' && exit 1; fi
    if [[ $(ps -ef | grep -c 'teamcity-agent') -gt 1 ]]; then echo 'TeamCity started.'; fi
    # if pgrep java; then echo 'TeamCity started.'; fi
done

echo -e "
Later, authorize this agent '$agent_name' on server, 

'${serverUrl}/agents/unauthorized'

After authorize, wait a few minutes for the agent to download some sources from the server,
check the progress in 
" "$HOME"/teamcity/teamcity-agent-*/logs/teamcity-agent.log
