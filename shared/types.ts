// Shared types for the job scheduler

export interface Job {
  id: string;
  status: JobStatus;
  payload?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export type JobStatus = "pending" | "running" | "completed" | "failed";
