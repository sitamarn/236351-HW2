using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    public interface ITreeView
    {
        public Dictionary<string, Uri> Snapshot { get; }
    }
}
