import React, { useEffect, useState, useRef } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { blue } from '@material-ui/core/colors';
import { Typography, TableContainer, Table, TableHead, TableRow, TableCell, Paper, TableBody, Switch } from '@material-ui/core';
import { Link } from 'react-router-dom';
import { getJobList } from '../../service/job';
import { Job } from '../../type/job';
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

const columns = ['ID', 'Name', 'Owner', 'Languages', 'Driver Entry'];

export default function JobList() {
  const classes = useStyles();
  const [jobList, setList] = useState<Job[]>([]);
  const [time, setTime] = useState(0);
  const [isRefreshing, setRefresh] = useState(true)
  const refreshRef = useRef(isRefreshing);
  refreshRef.current = isRefreshing
  const getJob = async () => {
    if (!refreshRef.current) {
      return;
    }
    const rsp = await getJobList();

    if (rsp?.data?.result) {
      setList(rsp.data.result)
      setTime(rsp.data.timestamp)
    }
  }

  useEffect(() => {
    getJob();
    const invId = setInterval(getJob, 2000);
    return () => {
      clearInterval(invId)
    }
  }, [])

  return <div className={classes.root}>
    <Typography variant="h5">
      Job List
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
          jobList.map(({id, name, owner, language, driverEntry}) => 
            <TableRow>
              <TableCell align="center">
                <Link to={`/job/${id}`}>{id}</Link>
              </TableCell>
              <TableCell align="center">
                {name}
              </TableCell>
              <TableCell align="center">
                {owner}
              </TableCell>
              <TableCell align="center">
                {language}
              </TableCell>
              <TableCell align="center">
                {driverEntry}
              </TableCell>
            </TableRow>
          )
        }
      </TableBody>
    </Table>
    </TableContainer>
  </div>
}
