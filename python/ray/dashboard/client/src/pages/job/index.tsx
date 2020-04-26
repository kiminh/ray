import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { blue } from '@material-ui/core/colors';
import { Typography, TableContainer, Table, TableHead, TableRow, TableCell, Paper, TableBody, Chip } from '@material-ui/core';
import { Link } from 'react-router-dom';

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

const columns = ['ID', 'Status', 'Name', 'MainClass'];


const data = [{
  id: '0x989',
  status: 'running',
  name: 'ray-mobius-test',
  mainClass: 'com.alipay.arc.mobius.some'
}
]

export default function Job() {
  const classes = useStyles()

  return <div className={classes.root}>
    <Typography variant="h5">
      Job List
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
          data.map(({id, name, status, mainClass}) => 
            <TableRow>
              <TableCell align="center">
                <Link to={`/job/${id}`}>{id}</Link>
              </TableCell>
              <TableCell align="center">
                <Chip size="small" variant="outlined" style={{ color: "green", borderColor: 'green' }} label={status.toUpperCase()}/>
              </TableCell>
              <TableCell align="center">
                {name}
              </TableCell>
              <TableCell align="center">
                {mainClass}
              </TableCell>
            </TableRow>
          )
        }
      </TableBody>
    </Table>
    </TableContainer>
  </div>
}
