import React from 'react';
import { makeStyles, Typography, Paper, Chip, Grid, TableContainer, TableHead, TableRow, TableCell, TableBody, Table, Tooltip } from '@material-ui/core';
import { RouteComponentProps } from 'react-router-dom';
import PercentageBar from '../../components/PercentageBar';
import numeral from 'numeral';
import WorkerTable from '../../components/WorkerTable';

const useStyle = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
  paper: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
    marginBottom: theme.spacing(2)
  },
  label: {
    fontWeight: 'bold'
  }
}))

const actorInfo = {
  status: 'running',
  id: '287483',
  name: 'test-actor',
  taskId: '1029',
  taskName: 'ray-test-task',
  stack: ['com.alipay.arch.BatchWorker.execute, org.ray.Executor.getMethod()'],
  error: ['FO Error'],
  resources: {"CPU": 1.0, "MEM": 1.0}
}

const job = {
  id: '0x989',
  status: 'running',
  name: 'ray-mobius-test',
  mainClass: 'com.alipay.arc.mobius.some'
}

const driverInfo = {
  status: 'RUNNING',
  pid: '59834',
  hostname: 'ray-test-1',
  port: '6500',
  cpu: 32,
  ram: 2339433300,
  ramTotal: 3200000000,
  disk: 4500223400,
  diskTotal: 6400000000,
  sent: 12345,
  recevied: 223563
}


export default function JobDetail(props: RouteComponentProps<{ id: string }>) {
  const { match: { params } } = props;
  const classes = useStyle();

  return <div className={classes.root}>
    <Typography variant="h5">
      Job - {params.id} <Chip size="small" variant="outlined" style={{ color: "green", borderColor: 'green' }} label={job.status.toUpperCase()}/>
    </Typography>
    <Paper className={classes.paper}>
    <Grid container spacing={2}>
        <Grid item xs><span className={classes.label}>name</span>: {job.name}</Grid>
        <Grid item xs><span className={classes.label}>mainClass</span>: {job.mainClass}</Grid>
      </Grid>
      <Grid container spacing={2}>
        <Grid item xs><span className={classes.label}>Other</span>: config</Grid>
        <Grid item xs><span className={classes.label}>Other</span>: config</Grid>
        <Grid item xs><span className={classes.label}>Other</span>: config</Grid>
      </Grid>
    </Paper>
    <Typography variant="h6">Packages</Typography>
    <TableContainer component={Paper} className={classes.paper}>
      <Table>
        <TableHead>
          <TableRow>
            {
              ['Type', 'Name', 'Version', 'Url'].map(col => <TableCell align="center">{col}</TableCell>)
            }
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell align="center">
              OSS
            </TableCell>
            <TableCell align="center">
              RayaG
            </TableCell>
            <TableCell align="center">
              2.1.1
            </TableCell>
            <TableCell align="center">
              oss://skfsdkfjksdjfksdjfk
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
    <Typography variant="h6">Driver Info</Typography>
    <TableContainer component={Paper} className={classes.paper}>
      <Table>
        <TableHead>
          <TableRow>
            {
              ['Status', 'Pid', 'Hostname', 'Port', 'CPU', 'RAM', 'Disk', 'Sent', 'Recevied'].map(col => <TableCell align="center">{col}</TableCell>)
            }
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>
              <Chip size="small" variant="outlined" label={driverInfo.status} style={{ color: "green", borderColor: 'green' }}/>
            </TableCell>
            <TableCell>
              {driverInfo.pid}
            </TableCell>
            <TableCell>
              {driverInfo.hostname}
            </TableCell>
            <TableCell>
              {driverInfo.port}
            </TableCell>
            <TableCell>
              <PercentageBar num={Number(driverInfo.cpu)} total={100}>
                {driverInfo.cpu}%
              </PercentageBar>
            </TableCell>
            <TableCell>
              <PercentageBar num={Number(driverInfo.ram)} total={Number(driverInfo.ramTotal)}>
                {numeral(driverInfo.ram).format('0.00b')}/{numeral(driverInfo.ramTotal).format('0.00b')}
              </PercentageBar>
            </TableCell>
            <TableCell>
              <PercentageBar num={Number(driverInfo.disk)} total={Number(driverInfo.diskTotal)}>
                {numeral(driverInfo.disk).format('0.00b')}/{numeral(driverInfo.diskTotal).format('0.00b')}
              </PercentageBar>
            </TableCell>
            <TableCell>
                {numeral(driverInfo.sent).format('0.00b')}/s
              </TableCell>
              <TableCell>
              {numeral(driverInfo.recevied).format('0.00b')}/s
              </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
    <Typography variant="h6">Worker Info</Typography>
      <TableContainer component={Paper} className={classes.paper}>
        <WorkerTable workers={[]} actorMap={{}}/>
      </TableContainer>
      <Typography variant="h6">Actor Info</Typography>
      <TableContainer component={Paper} className={classes.paper}>

      <Table>
  <TableHead>
    <TableRow>
      {
        'Status, Actor ID, Actor Name, Task ID, Task Name, Stack, Error, Resources'.split(',').map(col => <TableCell align="center">{col}</TableCell>)
      }
    </TableRow>
  </TableHead>
  <TableBody>
    <TableCell align="center">
      <Chip size="small" variant="outlined" label={actorInfo.status} style={{ color: "green", borderColor: 'green' }}/>
    </TableCell>
    <TableCell align="center">
      {actorInfo.id}
    </TableCell>
    <TableCell align="center">
      {actorInfo.name}
    </TableCell>
    <TableCell align="center">
      {actorInfo.taskId}
    </TableCell>
    <TableCell align="center">
      {actorInfo.taskName}
    </TableCell>
    <TableCell align="center">
      <Tooltip title={actorInfo.stack.join('\n')}>
        <div>{actorInfo.stack.length}</div>
      </Tooltip>
    </TableCell>
    <TableCell align="center">
    <Tooltip title={actorInfo.error.join('\n')}>
        <div>{actorInfo.error.length}</div>
      </Tooltip>
    </TableCell>
    <TableCell align="center">
      {JSON.stringify(actorInfo.resources, null, 2)}
    </TableCell>
  </TableBody>
</Table>
</TableContainer>
  </div>
}
