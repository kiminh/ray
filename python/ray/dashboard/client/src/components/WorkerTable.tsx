import React, { PropsWithChildren, ReactNode } from 'react';
import { Table, TableHead, TableRow, TableCell, TableBody, IconButton, Tooltip } from '@material-ui/core';
import { KeyboardArrowDown, KeyboardArrowRight } from '@material-ui/icons';
import PercentageBar from './PercentageBar';
import numeral from 'numeral';
import { Worker, CoreWorkerStats } from '../type/worker';
import { Actor } from '../type/actor';
import moment from 'moment';
import { StatusChip } from './StatusChip';

export const longTextCut = (text: string) => <Tooltip title={text}>
  <span>{text.length > 20 ? text.slice(0, 20) + '...' : text}</span>
</Tooltip>

const ExpandableTableRow = ({ children, expandComponent, ...otherProps }: PropsWithChildren<{ expandComponent: ReactNode }>) => {
  const [isExpanded, setIsExpanded] = React.useState(false);

  return (
    <>
      <TableRow {...otherProps}>
        <TableCell padding="checkbox">
          <IconButton onClick={() => setIsExpanded(!isExpanded)}>
            {isExpanded ? <KeyboardArrowDown /> : <KeyboardArrowRight />}
          </IconButton>
        </TableCell>
        {children}
      </TableRow>
      {isExpanded && (
        <TableRow>
          <TableCell colSpan={24}>{expandComponent}</TableCell>
        </TableRow>
      )}
    </>
  );
};

const WorkerDetailTable = ({ actorMap, coreWorkerStats }: { actorMap: { [actorId: string]: Actor }, coreWorkerStats: CoreWorkerStats[] }) => {
  const actors = coreWorkerStats.filter(e => actorMap[e.actorId]).map(e => ({ ...e, ...actorMap[e.actorId] }))

  if (!actors?.length) {
    return <p>The Worker Haven't Had Related Actor Yet.</p>
  }

  return <Table>
    <TableHead>
      <TableRow>
        {
          ['ActorID', 'Actor Title', 'Task Func Desc', 'Job Id', 'Pid', 'Port', 'State', 'Task Queue'].map(col => <TableCell align="center">{col}</TableCell>)
        }
      </TableRow>
    </TableHead>
    <TableBody>
      {
        actors.map(({ actorId, currentTaskFuncDesc, jobId, pid, port, state, taskQueueLength, actorTitle }) => <TableRow>
          <TableCell align="center">
            {actorId}
          </TableCell>
          <TableCell align="center">
            {actorTitle}
          </TableCell>
          <TableCell align="center">
            {longTextCut(currentTaskFuncDesc)}
          </TableCell>
          <TableCell align="center">
            {jobId}
          </TableCell>
          <TableCell align="center">
            {pid}
          </TableCell>
          <TableCell align="center">
            {port}
          </TableCell>
          <TableCell align="center">
            <StatusChip type="actor" status={state} />
          </TableCell>
          <TableCell align="center">
            {taskQueueLength}
          </TableCell>
        </TableRow>)
      }
    </TableBody>
  </Table>
}

const byteFmt = (val: number) => numeral(val).format('0.00b')

export default function WorkerTable({ workers, actorMap }: { workers: Worker[], actorMap: { [actorId: string]: Actor } }) {
  return <Table>
    <TableHead>
      <TableRow>
        {
          ['', 'Pid', 'CPU', 'CPU Times (user/system/iowait)', 'Memory (rss/vms/shared/text/lib/data/dirty)', 'CMD Line', 'Create Time'].map(col => <TableCell align="center">{col}</TableCell>)
        }
      </TableRow>
    </TableHead>
    <TableBody>
      {
        workers.map(({ pid, cpuPercent, cpuTimes, memoryInfo, cmdline, createTime, coreWorkerStats }) =>
          <ExpandableTableRow expandComponent={
            <WorkerDetailTable actorMap={actorMap} coreWorkerStats={coreWorkerStats} />
          }>
            <TableCell align="center">
              {pid}
            </TableCell>
            <TableCell align="center">
              <PercentageBar num={Number(cpuPercent * 100)} total={100}>
                {cpuPercent * 100}%
          </PercentageBar>
            </TableCell>
            <TableCell align="center">
              {cpuTimes.user}/{cpuTimes.system}/{cpuTimes.iowait}
            </TableCell>
            <TableCell align="center">
              {byteFmt(memoryInfo.rss)}/{byteFmt(memoryInfo.vms)}/{byteFmt(memoryInfo.shared)}/{byteFmt(memoryInfo.text)}/{byteFmt(memoryInfo.lib)}/{byteFmt(memoryInfo.data)}/{byteFmt(memoryInfo.dirty)}
            </TableCell>
            <TableCell align="center">
              {cmdline && cmdline.filter(e => e).join('\n')}
            </TableCell>
            <TableCell align="center">
              {moment(createTime * 1000).format('YYYY/MM/DD HH:mm:ss')}
            </TableCell>
          </ExpandableTableRow>
        )
      }
    </TableBody>
  </Table>
}
