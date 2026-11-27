using System.Runtime.CompilerServices;

// Fast metric tests live in the existing Analytics integration test project;
// the production observability helpers remain internal to the worker.
[assembly: InternalsVisibleTo("ConferenceProFlow.Analytics.IntegrationTests")]
