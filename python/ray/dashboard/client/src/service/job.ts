import axios from 'axios';
import { JobListRsp, JobDetailRsp } from '../type/job';

export function getJobList() {
  return axios.get<JobListRsp>('/jobs?view=summary');
}

export function getJobDetail(id: string) {
  return axios.get<JobDetailRsp>(`/jobs/${id}`);
}
