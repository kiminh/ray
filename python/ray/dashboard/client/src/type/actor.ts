export enum ActorEnum {
  ALIVE = 'ALIVE',
  PENDING = 'PENDING',
  RECONSTRUCTING = 'RECONSTRUCTING',
  DEAD = 'DEAD'
}

export interface Actor {
  "actorId": string,
  "children": { [key: string]: Actor },
  "currentTaskFuncDesc": string,
  "ipAddress": string,
  "isDirectCall": boolean,
  "jobId": string,
  "numExecutedTasks": number,
  "numLocalObjects": number,
  "numObjectIdsInScope": number,
  "port": string,
  "state": ActorEnum, // PENDING, ALIVE, RECONSTRUCTING, DEAD
  "taskQueueLength": number,
  "usedObjectStoreMemory": number,
  "usedResources": { [key: string]: string },
  "timestamp": number,
  "actorTitle": string,
  "averageTaskExecutionSpeed": number,
  "nodeId": string,
  "pid": number
}