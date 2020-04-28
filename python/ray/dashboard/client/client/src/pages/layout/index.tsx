import React, { PropsWithChildren } from 'react';
import classnames from 'classnames';
import { makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import Typography from '@material-ui/core/Typography';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import blueGrey from '@material-ui/core/colors/blueGrey';
import blue from '@material-ui/core/colors/blue';
import { RouteComponentProps } from 'react-router-dom';

import Logo from '../../logo.svg'


const drawerWidth = 200;

const useStyles = makeStyles((theme) => ({
  root: {
    display: 'flex',
  },
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
    color: 'white'
  },
  drawerPaper: {
    width: drawerWidth,
    background: blueGrey[900],
    boxShadow: `${blueGrey[600]} 0 2px 6px`
  },
  title: {
    padding: theme.spacing(2),
    color: 'white',
    textAlign: 'center',
    lineHeight: '36px'
  },
  white: {
    color: 'white'
  },
  divider: {
    background: 'rgba(255, 255, 255, .12)'
  },
  menuItem: {
    '&:hover': {
      background: blue["500"]
    },
  },
  selected: {
    background: blue["500"]
  },
  child: {
    flex: 1,
  }
}));

export default function BasicLayout(props: PropsWithChildren<RouteComponentProps>) {
  const classes = useStyles();
  const { location, history, children } = props;

  return <div className={classes.root}>
    <Drawer
      variant="permanent"
      anchor="left"
      className={classes.drawer}
      classes={{
        paper: classes.drawerPaper,
      }}
    >
      <Typography variant="h6" className={classes.title}>
        <img width={36} src={Logo} alt="Ray"/> <br/> Ray DashBoard
      </Typography>
      <List>
        <ListItem button className={classnames(classes.menuItem, location.pathname.includes('node') && classes.selected)} onClick={() => history.push('/node')}>
          <ListItemText className={classes.white}>Node View</ListItemText>
        </ListItem>
        <ListItem button className={classnames(classes.menuItem, location.pathname.includes('job') && classes.selected)} onClick={() => history.push('/job')}>
        <ListItemText className={classes.white}>Job View</ListItemText>
        </ListItem>
      </List>
    </Drawer>
    <div className={ classes.child }>
      {children}
    </div>
  </div>
}
