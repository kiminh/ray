import React, { PropsWithChildren } from 'react';

export default function PercentageBar(props: PropsWithChildren<{num: number, total: number}>) {
  const { num, total } = props;
  const per = Math.round(num / total * 100);

  return <div
    style={{
      background: "linear-gradient(to right, #12c2e988, #c471ed88, #f64f59aa)",
      padding: '0 12px',
      height: 24,
      lineHeight: '24px',
      position: "relative",
      border: '#eee solid 1px',
      boxSizing: 'content-box',
      transition: '0.5s width'
    }}
  >
    <div
      style={{
        width: `${Math.min(Math.max(0, 100 - per), 100)}%`,
        background: 'white',
        position: 'absolute',
        right: 0,
        height: 24,
        transition: '0.5s width'
      }}
    >
    </div>
    <div style={{ zIndex: 2, position: 'relative', color: 'rgba(0, 0, 0, .75)', width: '100%', textAlign: 'center' }}>
      {props.children}
    </div>
  </div>
}