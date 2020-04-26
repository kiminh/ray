import { NodeListRsp, NodeDetailExtend, NodeDetailRsp } from "../type/node";

export const nodeList = {
  "result": [
    {
      "now": 1587002683.667489,
      "hostname": "liubaodeMacBook-Pro.local",
      "ip": "192.168.50.75",
      "cpu": 11.5,
      "cpus": [
        8,
        4
      ],
      "mem": [
        17179869184,
        6096281600,
        64.5
      ],
      "boot_time": 1585794560.0,
      "load_avg": [
        [
          8.9658203125,
          7.53955078125,
          5.810546875
        ],
        [
          1.12,
          0.94,
          0.73
        ]
      ],
      "disk": {
        "/": {
          "total": 250790436864,
          "used": 186368114688,
          "free": 56200609792,
          "percent": 76.8
        },
        "/tmp": {
          "total": 250790436864,
          "used": 186368114688,
          "free": 56200609792,
          "percent": 76.8
        }
      },
      "net": [
        5541.917494703708,
        2604.0335216077665
      ],
      "raylet": {
        "numWorkers": 9,
        "nodeId": "6afd2b465b156260c3d07e18a98bdee7e4aff6cf"
      }
    }
  ],
  "timestamp": 1587002684.472066,
  "error": null
} as NodeListRsp;

export const nodeDetail = {
  "result": {
    "now": 1587037808.933929,
    "hostname": "liubaodeMacBook-Pro.local",
    "ip": "192.168.50.75",
    "cpu": 18.1,
    "cpus": [
      8,
      4
    ],
    "mem": [
      17179869184,
      5626576896,
      67.2
    ],
    "workers": [
      {
        "create_time": 1587025515.213356,
        "cpu_percent": 1.8,
        "cmdline": [
          "ray::B",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          ""
        ],
        "memory_info": {
          "rss": 25387008,//  aka “Resident Set Size”, this is the non-swapped physical memory a process has used. On UNIX it matches “top“‘s RES column). On Windows this is an alias for wset field and it matches “Mem Usage” column of taskmgr.exe.
          "vms": 6877040640, // aka “Virtual Memory Size”, this is the total amount of virtual memory used by the process. On UNIX it matches “top“‘s VIRT column. On Windows this is an alias for pagefile field and it matches “Mem Usage” “VM Size” column of taskmgr.exe.
          "pfaults": 15472, // number of page faults.
          "pageins": 34 // number of actual pageins.
        },
        "cpu_times": {
          "user": 159.425232896,
          "system": 50.87182848,
          "children_user": 0.0,
          "children_system": 0.0
        },
        "pid": 46240,
        "coreWorkerStats": [
          {
            "currentTaskFuncDesc": "{type=PythonFunctionDescriptor, module_name=__main__, class_name=, function_name=bar, function_hash=96314f24df85a18abf207527482f2e625759a4cb}",
            "ipAddress": "192.168.50.75",
            "port": "58834",
            "actorId": "////////",
            "usedResources": {
              "CPU": 1.0
            },
            "numExecutedTasks": 1,
            "workerId": "OyBtgDf1/o3sxSziEOYPQgYpbT4="
          }
        ]
      }
    ],
    "boot_time": 1585794560.0,
    "load_avg": [
      [
        6.81494140625,
        6.744140625,
        6.36572265625
      ],
      [
        0.85,
        0.84,
        0.8
      ]
    ],
    "disk": {
      "/": {
        "total": 250790436864,
        "used": 186767454208,
        "free": 54727507968,
        "percent": 77.3
      },
      "/tmp": {
        "total": 250790436864,
        "used": 186767454208,
        "free": 54727507968,
        "percent": 77.3
      }
    },
    "net": [
      4356.234177703064,
      32973.34177584473
    ],
    "log_counts": 0,
    "error_counts": 0,
    "raylet": {
      "viewData": [
        {
          "viewName": "task_count_received",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75",
              "intValue": "1"
            }
          ]
        },
        {
          "viewName": "actor_stats",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:reconstructing_actors"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:live_actors",
              "doubleValue": 1.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:dead_actors"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:max_num_handles"
            }
          ]
        },
        {
          "viewName": "task_dependency_manager_stats",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_task_dependencies"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_required_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_pending_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_required_objects"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_local_objects"
            }
          ]
        },
        {
          "viewName": "reconstruction_policy_stats",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_reconstructing_tasks"
            }
          ]
        },
        {
          "viewName": "lineage_cache_stats",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_children"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_subscribed_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_lineages"
            }
          ]
        },
        {
          "viewName": "redis_latency",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,CustomKey:",
              "distributionMin": 404.0,
              "distributionMean": 404.0,
              "distributionMax": 404.0,
              "distributionCount": 1.0,
              "distributionBucketBoundaries": [
                100.0,
                200.0,
                300.0,
                400.0,
                500.0,
                600.0,
                700.0,
                800.0,
                900.0,
                1000.0
              ],
              "distributionBucketCounts": [
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0
              ]
            }
          ]
        },
        {
          "viewName": "current_driver",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,DriverPid:",
              "doubleValue": 46211.0
            }
          ]
        },
        {
          "viewName": "current_worker",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46239",
              "doubleValue": 46239.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46242",
              "doubleValue": 46242.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46236",
              "doubleValue": 46236.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46243",
              "doubleValue": 46243.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46240",
              "doubleValue": 46240.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46238",
              "doubleValue": 46238.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46237",
              "doubleValue": 46237.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,Language:PYTHON,WorkerPid:46241",
              "doubleValue": 46241.0
            }
          ]
        },
        {
          "viewName": "object_manager_stats",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:used_object_store_memory"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_local_objects"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_active_wait_requests"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_unfulfilled_push_requests"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_pull_requests"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_profile_events"
            }
          ]
        },
        {
          "viewName": "local_total_resource",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:memory",
              "doubleValue": 89.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:node:192.168.50.75",
              "doubleValue": 1.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:CPU",
              "doubleValue": 8.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:object_store_memory",
              "doubleValue": 30.0
            }
          ]
        },
        {
          "viewName": "scheduling_queue_stats",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_placeable_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_waiting_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_ready_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_waiting for actor creation_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_infeasible_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_running_tasks"
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ValueType:num_swap_tasks"
            }
          ]
        },
        {
          "viewName": "local_available_resource",
          "measures": [
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:memory",
              "doubleValue": 89.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:node:192.168.50.75",
              "doubleValue": 1.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:CPU",
              "doubleValue": 8.0
            },
            {
              "tags": "JobName:raylet,Version:0.9.0.dev0,NodeAddress:192.168.50.75,ResourceName:object_store_memory",
              "doubleValue": 30.0
            }
          ]
        }
      ],
      "numWorkers": 9,
      "pid": 46233,
      "nodeId": "7a9949d83f58d1816dea021ea92e243ce4e9bedc",
      "nodeManagerPort": 50489
    },
    "actors": {
      "45b95b1c0100": {
        "actorId": "45b95b1c0100",
        "children": {},
        "currentTaskFuncDesc": "{type=PythonFunctionDescriptor, module_name=__main__, class_name=A, function_name=foo, function_hash=}",
        "ipAddress": "192.168.50.75",
        "isDirectCall": true,
        "jobId": "0100",
        "numExecutedTasks": 2,
        "numLocalObjects": 0,
        "numObjectIdsInScope": 0,
        "port": "58837",
        "state": "ALIVE", // PENDING, ALIVE, RECONSTRUCTING, DEAD
        "taskQueueLength": 0,
        "usedObjectStoreMemory": 0,
        "usedResources": {},
        "timestamp": 0.0,
        "actorTitle": "A([], {})",
        "averageTaskExecutionSpeed": 0.0,
        "nodeId": "0035493ea86a2597cc1b26ffdd2249e7476031bb",
        "pid": 64134
      }
    }
  },
  "timestamp": 1587037809.845348,
  "error": null
};
