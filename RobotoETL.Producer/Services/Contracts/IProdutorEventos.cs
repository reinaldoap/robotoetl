using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RobotoETL.Producer.Services.Contracts
{
    public interface IProdutorEventos
    {

        Task ProduzirEventosAsync();
    }
}
