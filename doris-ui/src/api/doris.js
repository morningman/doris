import { req } from './axiosrequest';
const baseurl='/';
export const login = (params) => { return req("get", baseurl + "rest/v1/system?path=//frontends", params) };

// system-frontend
export const frontendList = (params) => { return req("get", baseurl + "rest/v1/system?path=//frontends", params) };
//system-brokers
export const brokersList = (params) => { return req("get", baseurl + "rest/v1/system?path=//brokers", params) };
//system-auth
export const authList = (params) => { return req("get", baseurl + "rest/v1/system?path=//auth", params) };
//system-auth-authInfo
export const authInfoList = (params) => { return req("get", baseurl + "rest/v1/system?path=//auth/"+ params.replace(/%/g, '%25'), params) };

//routine load
export const routineloadList = (params) => { return req("get", baseurl + "rest/v1/system?path=//routine_loads", params) };
//jobs
export const jobsList = (params) => { return req("get", baseurl + "rest/v1/system?path=//jobs", params) };
//resource list
export const resourceList = (params) => { return req("get", baseurl + "rest/v1/system?path=//resources", params) };

//monitor list
export const monitorList = (params) => { return req("get", baseurl + "rest/v1/system?path=//monitor", params) };
//monitor info list
export const monitorInfoList = (params) => { return req("get", baseurl + "rest/v1/system?path=//monitor/"+params.replace(/%/g, '%25'), params) };

//transactions list
export const transationsList = (params) => { return req("get", baseurl + "rest/v1/system?path=//transactions", params) };
export const transations_op_List = (params) => { return req("get", baseurl + "rest/v1/system?path=//transactions/"+params.replace(/%/g, '%25'), params) };

export const colocationList = (params) => { return req("get", baseurl + "rest/v1/system?path=//colocation_group", params) };

export const backendsList = (params) => { return req("get", baseurl + "rest/v1/system?path=//backends", params) };
export const backendInfoList = (params) => { return req("get", baseurl + "rest/v1/system?path=//backends/"+params.replace(/%/g, '%25'), params) };
export const cluster_balance_List = (params) => { return req("get", baseurl + "rest/v1/system?path=//cluster_balance", params) };
export const cluster_balance_sub_List = (params) => { return req("get", baseurl + "rest/v1/system?path=//cluster_balance/"+params.replace(/%/g, '%25'), params) };


export const queryList = (params) => { return req("get", baseurl + "rest/v1/query", params) };
export const queryProfileList = (params) => { return req("get", baseurl + "rest/v1/query_profile/"+params.replace(/%/g, '%25'), params) };

export const sessionList = (params) => { return req("get", baseurl + "rest/v1/session", params) };

export const variableList = (params) => { return req("get", baseurl + "rest/v1/variable", params) };

export const logList = (params) => { return req("get", baseurl + "rest/v1/log" + params.replace(/%/g, '%25'), params) };

export const haList = (params) => { return req("get", baseurl + "rest/v1/ha", params) };
export const systemInfoList = (params) => { return req("get", baseurl + "rest/v1/index", params) };
export const current_queries_List = (params) => { return req("get", baseurl + "rest/v1/system?path=//current_queries", params) };

export const current_queries_info_list = (params) => { return req("get", baseurl + "rest/v1/system?path=//current_queries/"+params.replace(/%/g, '%25'), params) };

export const dbslist = (params) => { return req("get", baseurl + "rest/v1/system?path=//dbs", params) };
export const dbs_info_list = (params) => { return req("get", baseurl + "rest/v1/system?path=//dbs/"+params.replace(/%/g, '%25'), params) };

export const table_info_list = (params) => { return req("get", baseurl + "rest/v1/system?"+params.replace(/%/g, '%25'), params) };

export const current_backend_instances_list = (params) => { return req("get", baseurl + "rest/v1/system?path=//current_backend_instances", params) };

export const tasks_list = (params) => { return req("get", baseurl + "rest/v1/system?path=//tasks", params) };

export const statistic_list = (params) => { return req("get", baseurl + "rest/v1/system?path=//statistic", params) };

export const helpList = (params) => { return req("get", baseurl + "rest/v1/help" + params.replace(/%/g, '%25'), params) };
