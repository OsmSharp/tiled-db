using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace OsmSharp.Db.Tiled.API.Controllers
{
    [ApiController]
    public class DbViewController : ControllerBase
    {
        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] {"value1", "value2"};
        }

        // GET api/values/5
        [HttpGet("{view}/")]
        public ActionResult<string> Get(int id)
        {
            return "value";
        }
    }
}