using System.Collections.Generic;

namespace DataFactoryDemo
{
    public class PipelineParameters
    {
        public string InputPath { get; set; }
        public string OutputPath { get; set; }

        public Dictionary<string, object> ToDictionary()
        {
            return new Dictionary<string, object>
            {
                [nameof(InputPath).ToLower()] = InputPath,
                [nameof(OutputPath).ToLower()] = OutputPath
            };
        }
    }
}