import axios from 'axios';
import { NodeListRsp, NodeDetailRsp } from '../type/node';

export async function getNodeList() {
  const rsp = await axios.get<NodeListRsp>('/nodes?view=summary')

  if (rsp?.data?.result?.length) {
    return { list: rsp.data.result, timestamp: rsp.data.timestamp }
  }
  
  return { list: [], timestamp: 0}
}

export async function getNodeDetail(hostname: string) {
  const rsp = await axios.get<NodeDetailRsp>(`/nodes/${hostname}`)

  if (rsp?.data) {
    return { detail: rsp.data.result, timestamp: rsp.data.timestamp };
  }

  return { detail: null, timestamp: 0 }
}

