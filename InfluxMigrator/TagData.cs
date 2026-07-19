using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InfluxdbDataSync
{
    public class TagData
    {
        public string TagId { get; set; } = string.Empty;

        public double Value { get; set; }

        public long Timestamp { get; set; }
    }
}
