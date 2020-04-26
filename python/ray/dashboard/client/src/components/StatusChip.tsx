import React from 'react';
import { ActorEnum } from '../type/actor';
import { Chip } from '@material-ui/core';
import { green, red, blue, lightBlue } from '@material-ui/core/colors';
import { CSSProperties } from '@material-ui/core/styles/withStyles';

const colorMap = {
  node: {
    RUNNING: green[500]
  },
  actor: {
    [ActorEnum.ALIVE]: green[500],
    [ActorEnum.DEAD]: red[500],
    [ActorEnum.PENDING]: blue[500],
    [ActorEnum.RECONSTRUCTING]: lightBlue[500]
  }
} as {
  [key: string]: {
    [key: string]: string
  }
}

export function StatusChip({ type, status }: { type: string, status: string | ActorEnum }) {
  const style = {} as CSSProperties;

  if (colorMap[type] && colorMap[type][status]) {
    style.color = colorMap[type][status];
    style.borderColor = colorMap[type][status];
  }

  return <Chip style={style} variant="outlined" size="small" label={status}/>
}
