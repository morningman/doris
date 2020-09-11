---
{
    "title": "HA ACTION",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# HA Action

## Request

```
GET /rest/v1/ha
```

## Description

HA Action 用于获取 FE 集群的高可用组信息。
    
## Path parameters

无

## Query parameters

无

## Request body

无

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"Observernodes": [],
		"CurrentJournalId": [{
			"Value": 233653,
			"Name": "FrontendRole"
		}],
		"Electablenodes": [{
			"Value": "xafj-palo-rpm64.xafj.baidu.com",
			"Name": "xafj-palo-rpm64.xafj.baidu.com"
		}],
		"allowedFrontends": [{
			"Value": "name: 10.81.85.89_9213_1597652404352, role: FOLLOWER, 10.81.85.89:9213",
			"Name": "10.81.85.89_9213_1597652404352"
		}],
		"removedFronteds": [],
		"CanRead": [{
			"Value": true,
			"Name": "Status"
		}],
		"databaseNames": [{
			"Value": "232384 ",
			"Name": "DatabaseNames"
		}],
		"FrontendRole": [{
			"Value": "MASTER",
			"Name": "FrontendRole"
		}],
		"CheckpointInfo": [{
			"Value": 232383,
			"Name": "Version"
		}, {
			"Value": "2020-08-26T07:54:48.000+0000",
			"Name": "lastCheckPointTime"
		}]
	},
	"count": 0
}
```
    