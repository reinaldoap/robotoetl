namespace RobotoETL.Model
{
    public class Pessoa
    {
        public required string Cpf { get; set; }
        public required string Nome { get; set; }
        public DateOnly Nascimento { get; set; }
        public required Sexo Sexo { get; set; }
    }
}
