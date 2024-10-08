
namespace RobotoETL.Model
{
    public class Prontuario
    {
        Guid Id { get; set; }
        public required Pessoa Paciente { get; set; }
        public DateTime DataConsulta { get; set; }
        public int PressaoCardiacaMin { get; set; }
        public int PressaoCardiacaMax { get; set; }
        public string? AvaliacaoMedica { get; set; }
    }
}
