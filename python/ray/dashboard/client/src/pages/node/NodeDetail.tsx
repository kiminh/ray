import React, { useState, useEffect, useRef } from 'react';
import { makeStyles, Typography, Paper, Grid, TableContainer, Switch} from '@material-ui/core';
import { RouteComponentProps } from 'react-router-dom';
import PercentageBar from '../../components/PercentageBar';
import numeral from 'numeral';
import moment from 'moment';
import WorkerTable from '../../components/WorkerTable';
import { NodeDetailExtend } from '../../type/node';
import { getNodeDetail } from '../../service/node';
import { StatusChip } from '../../components/StatusChip';
import { ViewMeasures } from '../../type/raylet';

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

const showMeasureKeys = ['local_total_resource', 'local_available_resource', 'actor_stats', 'task_dependency_manager_stats', 'reconstruction_policy_stats', 'scheduling_queue_stats', 'object_manager_stats']

function ViewDataDisplayer({ view }: { view?: ViewMeasures }) {
  if (!view) {
    return null;
  }
  const { tags = "", ...otherProps } = view;

  return <Grid item xs>
    <span>{tags.split(',').pop()?.split(':').pop()?.split('_').map(e => e[0].toUpperCase() + e.slice(1)).join(' ')}</span>:{' '}
    {
      Object.keys(otherProps).length > 1 ?
        Object.entries(otherProps).map(e => e.join('=')).join(' | ') :
        Object.values(otherProps)[0] || <span style={{ color: 'gray' }}>null</span>
    }
  </Grid>
}

export default function NodeDetailPage(props: RouteComponentProps<{ id: string }>) {
  const { match: { params } } = props;
  const [nodeDetail, setNode] = useState<NodeDetailExtend | undefined>();
  const [time, setTime] = useState(0);
  const [isRefreshing, setRefresh] = useState(true)
  const refreshRef = useRef(isRefreshing);
  refreshRef.current = isRefreshing
  const getDetail = async () => {
    if (!refreshRef.current) {
      return;
    }
    const { detail, timestamp } = await getNodeDetail(params.id);
    if (detail) {
      setNode(detail);
      setTime(timestamp)
    }
  }
  const classes = useStyle();
  const raylet = nodeDetail?.raylet;

  useEffect(() => {
    getDetail()
    const invId = setInterval(getDetail, 3000);
    return () => {
      clearInterval(invId);
    }
  }, [])

  return <div className={classes.root}>
    <Typography variant="h5">
      Node - {params.id} <StatusChip type="node" status="RUNNING" />
    </Typography>
    <Typography>
      Last Refresh Time: {moment(time * 1000).format('YYYY/MM/DD HH:mm:ss')} <br />
      Refresh:
    <Switch
        checked={isRefreshing}
        onChange={(event: React.ChangeEvent<HTMLInputElement>) => { setRefresh(event.target.checked) }}
        name="refresh"
        inputProps={{ 'aria-label': 'secondary checkbox' }}
      />
    </Typography>
    {
      nodeDetail &&
      <Paper className={classes.paper}>
        <Grid container spacing={2}>
          <Grid item xs><span className={classes.label}>Hostname</span>: {nodeDetail.hostname}</Grid>
          <Grid item xs><span className={classes.label}>IP</span>: {nodeDetail.ip}</Grid>
          <Grid item xs><span className={classes.label}>CPU (Logic/Physic)</span>: {nodeDetail.cpus[0]}/ {nodeDetail.cpus[1]}</Grid>
        </Grid>
        <Grid container spacing={2}>
          <Grid item xs><span className={classes.label}>Load (1/5/15min)</span>: {nodeDetail?.load_avg[0] && nodeDetail.load_avg[0].map(e => Number(e).toFixed(2)).join('/')}</Grid>
          <Grid item xs><span className={classes.label}>Load per CPU (1/5/15min)</span>: {nodeDetail?.load_avg[1] && nodeDetail.load_avg[1].map(e => Number(e).toFixed(2)).join('/')}</Grid>
          <Grid item xs><span className={classes.label}>Boot Time</span>: {moment(nodeDetail.boot_time * 1000).format('YYYY/MM/DD HH:mm:ss')}</Grid>
        </Grid>
        <Grid container spacing={2}>
          <Grid item xs><span className={classes.label}>Sent Tps</span>: {numeral(nodeDetail?.net[0]).format('0.00b')}/s</Grid>
          <Grid item xs><span className={classes.label}>Recieved Tps</span>: {numeral(nodeDetail?.net[1]).format('0.00b')}/s</Grid>
          <Grid item xs></Grid>
        </Grid>
        <Grid container spacing={2}>
          <Grid item xs><span className={classes.label}>Memory</span>: <PercentageBar num={Number(nodeDetail?.mem[0] - nodeDetail?.mem[1])} total={nodeDetail?.mem[0]}>
            {numeral(nodeDetail?.mem[0] - nodeDetail?.mem[1]).format('0.00b')}/{numeral(nodeDetail?.mem[0]).format('0.00b')}({nodeDetail?.mem[2]}%)
                </PercentageBar></Grid>
          <Grid item xs><span className={classes.label}>CPU</span>: <PercentageBar num={Number(nodeDetail.cpu)} total={100}>
            {nodeDetail.cpu}%
                </PercentageBar></Grid>
        </Grid>
        <Grid container spacing={2}>
          {
            nodeDetail?.disk && Object.entries(nodeDetail?.disk).map(([path, obj]) =>
              <Grid item xs={6}><span className={classes.label}>Disk ({path})</span>: {
                obj &&
                <PercentageBar num={Number(obj.used)} total={obj.total}>
                  {numeral(obj.used).format('0.00b')}/{numeral(obj.total).format('0.00b')}({obj.percent}%, {numeral(obj.free).format('0.00b')} free)
                    </PercentageBar>
              }</Grid>
            )
          }
        </Grid>
      </Paper>
    }
    {
      raylet &&
      <>
        <Typography variant="h6">Raylet Info</Typography>
        <Paper className={classes.paper}>
          <Grid container spacing={2}>
            <Grid item xs>
              <span className={classes.label}>Pid</span>: {raylet?.pid}
            </Grid>
            <Grid item xs>
              <span className={classes.label}>Workers Num</span>: {raylet?.numWorkers}
            </Grid>
            <Grid item xs>
              <span className={classes.label}>Node Manager Port</span>: {raylet?.nodeManagerPort}
            </Grid>
          </Grid>
          {
            showMeasureKeys.map(e => raylet.viewData.find(view => view.viewName === e)).map(e => e ? <>
              <p className={classes.label}>{e.viewName.split('_').map(e => e[0].toUpperCase() + e.slice(1)).join(' ')}</p>
              <Grid container spacing={2}>
                {
                  e.measures.map(e => <ViewDataDisplayer view={e} />)
                }
              </Grid>
            </> : null
            )
          }

        </Paper>
      </>
    }
    {/* <Typography variant="h6">Object Store Info</Typography>
    <TableContainer component={Paper} className={classes.paper}>
      <Table>
        <TableHead>
          <TableRow>
            {
              ['Status', 'Pid', 'IPC Sock', 'Port', 'CPU', 'RAM', 'Disk', 'Sent', 'Recevied'].map(col => <TableCell align="center">{col}</TableCell>)
            }
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>
              <Chip size="small" variant="outlined" label={objectStroreInfo.status} style={{ color: "green", borderColor: 'green' }} />
            </TableCell>
            <TableCell>
              {objectStroreInfo.pid}
            </TableCell>
            <TableCell>
              {objectStroreInfo.ipc}
            </TableCell>
            <TableCell>
              {objectStroreInfo.port}
            </TableCell>
            <TableCell>
              <PercentageBar num={Number(objectStroreInfo.cpu)} total={100}>
                {objectStroreInfo.cpu}%
              </PercentageBar>
            </TableCell>
            <TableCell>
              <PercentageBar num={Number(objectStroreInfo.ram)} total={Number(objectStroreInfo.ramTotal)}>
                {numeral(objectStroreInfo.ram).format('0.00b')}/{numeral(objectStroreInfo.ramTotal).format('0.00b')}
              </PercentageBar>
            </TableCell>
            <TableCell>
              <PercentageBar num={Number(objectStroreInfo.disk)} total={Number(objectStroreInfo.diskTotal)}>
                {numeral(objectStroreInfo.disk).format('0.00b')}/{numeral(objectStroreInfo.diskTotal).format('0.00b')}
              </PercentageBar>
            </TableCell>
            <TableCell>
              {numeral(objectStroreInfo.sent).format('0.00b')}/s
              </TableCell>
            <TableCell>
              {numeral(objectStroreInfo.recevied).format('0.00b')}/s
              </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer> */}
    {
      nodeDetail?.workers &&
      <>
        <Typography variant="h6">Worker Info</Typography>
        <TableContainer component={Paper} className={classes.paper}>
          <WorkerTable workers={nodeDetail?.workers} actorMap={nodeDetail?.actors} />
        </TableContainer>
      </>
    }
  </div>
}
