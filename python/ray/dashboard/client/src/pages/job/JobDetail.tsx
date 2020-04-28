import React, { useState, useEffect } from 'react';
import { makeStyles, Typography, Paper, Grid, TableContainer, TableHead, TableRow, TableCell, TableBody, Table, Switch } from '@material-ui/core';
import { RouteComponentProps } from 'react-router-dom';
import WorkerTable, { longTextCut } from '../../components/WorkerTable';
import { JobDetail } from '../../type/job';
import { getJobDetail } from '../../service/job';
import { StatusChip } from '../../components/StatusChip';
import moment from 'moment';

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

export default function JobDetailPage(props: RouteComponentProps<{ id: string }>) {
  const { match: { params } } = props;
  const [job, setJob] = useState<JobDetail>();
  const [time, setTime] = useState(0);
  const [refreshing, setRefresh] = useState(true);
  const classes = useStyle();
  const getJob = async () => {
    if (!refreshing) {
      return;
    }
    const rsp = await getJobDetail(params.id);

    if (rsp.data) {
      setJob(rsp.data.result);
      setTime(rsp.data.timestamp);
    }
  }

  useEffect(() => {
    getJob();
  }, [])

  useEffect(() => {
    const itvId = setInterval(getJob, 3000);
    return () => clearInterval(itvId);
  }, [getJob])

  if (!job || !job.jobInfo.name) {
    return <div className={classes.root}>
      <Typography variant="h5">
        Job - {params.id}
      </Typography>
    </div>
  }

  const { jobInfo } = job;
  const actorMap = Object.fromEntries(job.jobActors.map(e => [e.actorId, e]))

  return <div className={classes.root}>
    <Typography variant="h5">
      Job - {params.id} <StatusChip type="job" status={jobInfo.state} />
    </Typography>
    <Typography>
      Last Refresh Time: {moment(time * 1000).format('YYYY/MM/DD HH:mm:ss')} <br />
      Refresh:
    <Switch
        checked={refreshing}
        onChange={(event: React.ChangeEvent<HTMLInputElement>) => { setRefresh(event.target.checked) }}
        name="refresh"
        inputProps={{ 'aria-label': 'secondary checkbox' }}
      />
    </Typography>
    <Paper className={classes.paper}>
      <Grid container spacing={2}>
        <Grid item xs={4}><span className={classes.label}>Name</span>: {jobInfo.name}</Grid>
        <Grid item xs={4}><span className={classes.label}>Owner</span>: {jobInfo.owner}</Grid>
        <Grid item xs={4}><span className={classes.label}>Language</span>: {jobInfo.language}</Grid>
        <Grid item xs={4}><span className={classes.label}>Driver Entry</span>: {jobInfo.driverEntry}</Grid>
        <Grid item xs={4}><span className={classes.label}>Driver Args</span>: {jobInfo.driverArgs.join(' ')}</Grid>
        <Grid item xs={4}><span className={classes.label}>JVM Options</span>: {jobInfo.jvmOptions}</Grid>
        <Grid item xs={4}><span className={classes.label}>Url</span>: {jobInfo.url}</Grid>
        {Object.entries(jobInfo.customConfig).map(([k, v]) => <Grid item xs={4}><span className={classes.label}>{k}</span>: {v}</Grid>)}
      </Grid>
    </Paper>
    {
      jobInfo.dependencies.python &&
      <>
        <Typography variant="h6">Python Dependencies</Typography>
        <Paper className={classes.paper}>
          <Grid container spacing={2}>
            {
              jobInfo.dependencies.python.map(e => {
                return <Grid item xs={4}>{e}</Grid>
              })
            }
          </Grid>
        </Paper>
      </>
    }
    {
      jobInfo.dependencies.java &&
      <>
        <Typography variant="h6">Python Dependencies</Typography>
        <TableContainer component={Paper} className={classes.paper}>
          <TableHead>
            <TableRow>
              {['Name', 'Version', 'URL'].map(col => <TableCell align="center">{col}</TableCell>)}
            </TableRow>
          </TableHead>
          <TableBody>
            {
              jobInfo.dependencies.java.map(({ name, version, url }) =>
                <TableRow>
                  <TableCell align="center">{name}</TableCell>
                  <TableCell align="center">{version}</TableCell>
                  <TableCell align="center">{url}</TableCell>
                </TableRow>
              )
            }
            
          </TableBody>
        </TableContainer>
      </>
    }
    <Typography variant="h6">Worker Info</Typography>
    <TableContainer component={Paper} className={classes.paper}>
      <WorkerTable workers={job.jobWorkers} actorMap={actorMap} />
    </TableContainer>
    <Typography variant="h6">Actor Info</Typography>
    <TableContainer component={Paper} className={classes.paper}>
      <Table>
        <TableHead>
          <TableRow>
            {
              ['ActorID', 'Actor Title', 'Task Func Desc', 'Job Id', 'Pid', 'Port', 'State', 'Task Queue'].map(col => <TableCell align="center">{col}</TableCell>)
            }
          </TableRow>
        </TableHead>
        <TableBody>
          {
            job.jobActors.map(({ actorId, currentTaskFuncDesc, jobId, pid, port, state, taskQueueLength, actorTitle }) => <TableRow>
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
    </TableContainer>
  </div>
}
