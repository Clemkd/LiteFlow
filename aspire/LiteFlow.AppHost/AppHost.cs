var builder = DistributedApplication.CreateBuilder(args);

// PostgreSQL container with a persistent data volume and the pgAdmin UI, so the workflow tables can be
// inspected while the fleet runs — liteflow.workflows and liteflow.workflow_steps are meant to be read.
var postgres = builder.AddPostgres("postgres")
    .WithImageTag("18")
    .WithDataVolume("liteflow-pgdata")
    .WithPgAdmin();

var db = postgres.AddDatabase("liteflowdb");

// Three replicas of the same worker, which is the deployment shape the library exists for: they compete
// for steps, an instance moves from one replica to another between steps without noticing, and killing a
// replica in the dashboard makes it resume on another one at the step it was on.
var workers = builder.AddProject<Projects.LiteFlow_Console>("liteflow-worker")
    .WithReference(db)
    .WaitFor(db)
    .WithReplicas(3)
    .WithArgs("worker", "--concurrency", "4");

// A one-shot producer that fills the engine with work for the replicas above to chew through.
builder.AddProject<Projects.LiteFlow_Console>("liteflow-seed")
    .WithReference(db)
    .WaitFor(db)
    .WaitFor(workers)
    .WithArgs("start", "--count", "50", "--fast");

builder.Build().Run();
