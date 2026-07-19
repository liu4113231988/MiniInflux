using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InfluxdbDataSync
{
    public class InfluxDataPoint
    {
        public string Measurement { get; set; } = string.Empty;
        public Dictionary<string, string> Tags { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, object> Fields { get; set; } = new Dictionary<string, object>();
        public long Timestamp { get; set; } // 毫秒级时间戳
    }
}
