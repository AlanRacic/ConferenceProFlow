var builder = WebApplication.CreateBuilder(args);

// Register application services.
builder.Services.AddOpenApi();
builder.Services.AddHealthChecks();

var app = builder.Build();

// Enable OpenAPI documentation in development.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Configure the HTTP request pipeline.
app.UseHttpsRedirection();

// Expose the application health endpoint.
app.MapHealthChecks("/health");

app.Run();

// Expose Program for integration testing.
public partial class Program { }