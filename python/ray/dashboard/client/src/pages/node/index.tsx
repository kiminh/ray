import React, { useState, useEffect, useRef } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { blue, green } from '@material-ui/core/colors';
import { Typography, TableContainer, Table, TableHead, TableRow, TableCell, Paper, TableBody, Chip, Switch, Divider } from '@material-ui/core';
import { Link } from 'react-router-dom';
import numeral from 'numeral';
import PercentageBar from '../../components/PercentageBar';
import { NodeDetail } from '../../type/node';
import { getNodeList } from '../../service/node';
import moment from 'moment';

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4),
    width: '100%',
  },
  table: {
    marginTop: theme.spacing(4),
    '& a': {
      color: blue["500"]
    }
  }
}));

const columns = ['Status', 'Host', 'IP', 'CPU Usage', 'Memory', 'Disk(root)', 'Sent', 'Received'];

export default function Node() {
  const classes = useStyles();
  const [nodeList, setList] = useState<NodeDetail[]>([]);
  const [time, setTime] = useState(0);
  const [isRefreshing, setRefresh] = useState(true)
  const refreshRef = useRef(isRefreshing);
  refreshRef.current = isRefreshing
  const getList = async () => {
    if (!refreshRef.current) {
      return
    }
    const { list, timestamp } = await getNodeList();
    setList(list);
    setTime(timestamp)
  }

  useEffect(() => {
    getList();
    const invId = setInterval(getList, 2000);
    return () => {
      clearInterval(invId)
    }
  }, [])

  return <div className={classes.root}>
    <Typography variant="h5">
      Node View
    </Typography>
    <Divider/>
    <Typography>
      Last Refresh Time: {moment(time * 1000).format('YYYY/MM/DD HH:mm:ss')} <br/>
      Refresh: 
    <Switch
        checked={isRefreshing}
        onChange={(event: React.ChangeEvent<HTMLInputElement>) => { setRefresh(event.target.checked) }}
        name="refresh"
        inputProps={{ 'aria-label': 'secondary checkbox' }}
      />
    </Typography>
    <TableContainer className={classes.table} component={Paper}>
    <Table>
      <TableHead>
        <TableRow>
          {
            columns.map(col => <TableCell align="center">{col}</TableCell>)
          }
        </TableRow>
      </TableHead>
      <TableBody>
        {
          nodeList.map(({ hostname, ip, cpu, mem, disk, net = [0, 0] }: NodeDetail) => 
            <TableRow>
              <TableCell>
                <Chip style={{ color: green[500], borderColor: green[500] }} label="RUNNING" variant="outlined" size="small"/>
              </TableCell>
              <TableCell align="center">
                <Link to={`/node/${hostname}`}>{hostname}</Link>
              </TableCell>
              <TableCell align="center">
                {ip}
              </TableCell>
              <TableCell>
                <PercentageBar num={Number(cpu)} total={100}>
                  {cpu}%
                </PercentageBar>
              </TableCell>
              <TableCell>
                <PercentageBar num={Number(mem[0] - mem[1])} total={mem[0]}>
                  {numeral(mem[0] - mem[1]).format('0.00b')}/{numeral(mem[0]).format('0.00b')}({mem[2]}%)
                </PercentageBar>
              </TableCell>
              <TableCell>
                {
                  disk['/'] &&
                  <PercentageBar num={Number(disk['/'].used)} total={disk['/'].total}>
                    {numeral(disk['/'].used).format('0.00b')}/{numeral(disk['/'].total).format('0.00b')}({disk['/'].percent}%)
                  </PercentageBar>
                }
              </TableCell>
              <TableCell align="center">
                {numeral(net[0]).format('0.00b')}/s
              </TableCell>
              <TableCell align="center">
                {numeral(net[1]).format('0.00b')}/s
              </TableCell>
            </TableRow>
          )
        }
      </TableBody>
    </Table>
    </TableContainer>
  </div>
}
