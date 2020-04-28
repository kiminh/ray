import { CssBaseline } from "@material-ui/core";
import { createMuiTheme, ThemeProvider } from '@material-ui/core/styles';
import React from "react";
import { Provider } from "react-redux";
import { HashRouter, Route } from "react-router-dom";
import Dashboard from "./pages/dashboard/Dashboard";
import { store } from "./store";
import BasicLayout from "./pages/layout";
import Job from "./pages/job";
import Node from "./pages/node";
import { teal, blue } from "@material-ui/core/colors";
import NodeDetail from "./pages/node/NodeDetail";
import JobDetail from "./pages/job/JobDetail";

const theme = createMuiTheme({
  palette: {
    primary: blue,
    secondary: teal,
  }
});


class App extends React.Component {
  render() {
    return (
      <ThemeProvider theme={theme}>
      <Provider store={store}>
        <CssBaseline />
        <HashRouter>
          <Route render={props => <BasicLayout {...props}>
            <Route component={Job} exact path="/job"/>
            <Route component={Node} exact path="/node"/>
            <Route component={NodeDetail} path="/node/:id"/>
            <Route component={JobDetail} path="/job/:id"/>
            <Route component={Dashboard} path="/v0" />
          </BasicLayout>}  path="/" />
        </HashRouter>
      </Provider>
      </ThemeProvider>
    );
  }
}

export default App;
