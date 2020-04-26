import { Worker } from "./worker";
import { Raylet } from "./raylet";
import { Actor } from "./actor";

export interface NodeDetail {
  "now": number,
  "hostname": string,
  "ip": string,
  "cpu": number,  // cpu usage
  "cpus": number[], // Logic CPU Count, Physical CPU Count
  "mem": number[], // total memory, free memory, memory used ratio
  "boot_time": number, // start time
  "load_avg": number[][], // recent 1，5，15 minitues system load，load per cpu http://man7.org/linux/man-pages/man3/getloadavg.3.html
  "disk": {
    // disk used on root
    "/": {
      "total": number,
      "used": number,
      "free": number,
      "percent": number
    },
    // disk used on tmp
    "/tmp": {
      "total": number,
      "used": number,
      "free": number,
      "percent": number
    }
  },
  "net": number[], // sent tps, received tps
  "raylet": {
    "numWorkers": number,
    "nodeId": string
  }
}

export interface NodeListRsp {
  result: NodeDetail[],
  timestamp: number,
  error: string | null,
}

export interface NodeDetailExtend extends NodeDetail {
  workers: Worker[],
  raylet: Raylet,
  actors: {
    [actorId: string]: Actor
  }
}

export interface NodeDetailRsp {
  result: NodeDetailExtend,
  timestamp: number,
  error: string | null,
}
