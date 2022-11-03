#!/bin/bash
set -e

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

DORIS_HOME="$teamcity_build_checkoutDir/output/"
data_home=${HOME}/teamcity/data/

if [[ ! -d "${data_home}" ]]; then mkdir -p "${data_home}"; fi

if ! mysql -h127.0.0.1 -P9030 -uroot -e'select @@version_comment'; then echo "Can't connect to doris..."; fi

echo "####create database and table"
mysql -h127.0.0.1 -P9030 -uroot -e "CREATE DATABASE IF NOT EXISTS hits"
sleep 10
mysql -h127.0.0.1 -P9030 -uroot hits -e"
CREATE TABLE IF NOT EXISTS  hits (
    CounterID INT NOT NULL, 
    EventDate DateV2 NOT NULL, 
    UserID BIGINT NOT NULL, 
    EventTime DateTimeV2 NOT NULL, 
    WatchID BIGINT NOT NULL, 
    JavaEnable SMALLINT NOT NULL,
    Title STRING NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    ClientIP INT NOT NULL,
    RegionID INT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL STRING NOT NULL,
    Referer STRING NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INT NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INT NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 STRING NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor VARCHAR(255) NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel STRING NOT NULL,
    Params STRING NOT NULL,
    IPNetworkID INT NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase STRING NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime DateTimeV2 NOT NULL,
    SilverlightVersion1 SMALLINT NOT NULL,
    SilverlightVersion2 SMALLINT NOT NULL,
    SilverlightVersion3 INT NOT NULL,
    SilverlightVersion4 SMALLINT NOT NULL,
    PageCharset STRING NOT NULL,
    CodeVersion INT NOT NULL,
    IsLink SMALLINT NOT NULL,
    IsDownload SMALLINT NOT NULL,
    IsNotBounce SMALLINT NOT NULL,
    FUniqID BIGINT NOT NULL,
    OriginalURL STRING NOT NULL,
    HID INT NOT NULL,
    IsOldCounter SMALLINT NOT NULL,
    IsEvent SMALLINT NOT NULL,
    IsParameter SMALLINT NOT NULL,
    DontCountHits SMALLINT NOT NULL,
    WithHash SMALLINT NOT NULL,
    HitColor CHAR NOT NULL,
    LocalEventTime DateTimeV2 NOT NULL,
    Age SMALLINT NOT NULL,
    Sex SMALLINT NOT NULL,
    Income SMALLINT NOT NULL,
    Interests SMALLINT NOT NULL,
    Robotness SMALLINT NOT NULL,
    RemoteIP INT NOT NULL,
    WindowName INT NOT NULL,
    OpenerName INT NOT NULL,
    HistoryLength SMALLINT NOT NULL,
    BrowserLanguage STRING NOT NULL,
    BrowserCountry STRING NOT NULL,
    SocialNetwork STRING NOT NULL,
    SocialAction STRING NOT NULL,
    HTTPError SMALLINT NOT NULL,
    SendTiming INT NOT NULL,
    DNSTiming INT NOT NULL,
    ConnectTiming INT NOT NULL,
    ResponseStartTiming INT NOT NULL,
    ResponseEndTiming INT NOT NULL,
    FetchTiming INT NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage STRING NOT NULL,
    ParamPrice BIGINT NOT NULL,
    ParamOrderID STRING NOT NULL,
    ParamCurrency STRING NOT NULL,
    ParamCurrencyID SMALLINT NOT NULL,
    OpenstatServiceName STRING NOT NULL,
    OpenstatCampaignID STRING NOT NULL,
    OpenstatAdID STRING NOT NULL,
    OpenstatSourceID STRING NOT NULL,
    UTMSource STRING NOT NULL,
    UTMMedium STRING NOT NULL,
    UTMCampaign STRING NOT NULL,
    UTMContent STRING NOT NULL,
    UTMTerm STRING NOT NULL,
    FromTag STRING NOT NULL,
    HasGCLID SMALLINT NOT NULL,
    RefererHash BIGINT NOT NULL,
    URLHash BIGINT NOT NULL,
    CLID INT NOT NULL
)  
DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
DISTRIBUTED BY HASH(UserID) BUCKETS 16
PROPERTIES ( \"replication_num\"=\"1\");
"

echo "####load data"
if [[ ! -f "${data_home}"/hits.tsv ]] || [[ $(wc -c "${data_home}"/hits.tsv | awk '{print $1}') != '74807831229' ]]; then
    cd "${data_home}"
    wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
    gzip -d hits.tsv.gz
    cd -
fi
row_count=$(mysql -h127.0.0.1 -P9030 -uroot -e "select count(*) from hits.hits" | sed -n '2p')
if [[ $row_count != 99997497 ]]; then
    date
    START=$(date +%s)
    echo "start loading ${i} ..."
    curl --location-trusted \
        -u root: \
        -T "${data_home}/hits.tsv" \
        -H "label:hits_${START}" \
        -H "columns: WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
        http://localhost:8030/api/hits/hits/_stream_load
    END=$(date +%s)
    LOADTIME=$(echo "$END - $START" | bc)
    echo "Load data costs $LOADTIME seconds"
    echo "$LOADTIME" >loadtime
    date
fi
du -bs "$DORIS_HOME"/be/storage/ | cut -f1 | tee storage_size
mysql -h127.0.0.1 -P9030 -uroot hits -e "SELECT count(*) FROM hits"
date

echo '-------------------------------------------------------------'
# bash get-segment-file-info.sh
echo '-------------------------------------------------------------'

echo "####run queries"
TRIES=3
QUERY_NUM=1
touch result.csv
truncate -s0 result.csv
OLD_IFS=$IFS
IFS=$'\n'
while read -r query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    echo -n "query${QUERY_NUM}," | tee -a result.csv
    for i in $(seq 1 $TRIES); do
        RES=$(mysql -vvv -h127.1 -P9030 -uroot hits -e "${query}" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)
        echo -n "${RES}" | tee -a result.csv
        [[ "$i" != "$TRIES" ]] && echo -n "," | tee -a result.csv
    done
    echo "" | tee -a result.csv
    QUERY_NUM=$((QUERY_NUM + 1))
done <queries.sql
IFS=$OLD_IFS

echo "####check query result"
set +e
if bash check-result.sh; then
    echo -e "
\033[32m
########################
check query result, PASS
########################
\033[0m"
    echo "####load and run, DONE."
    exit 0
else
    echo -e "
\033[31m
########################
check query result, FAIL
########################
\033[0m"
    echo "####load and run, DONE."
    exit 1
fi
