
using System;

namespace RobotoETL.Model
{
    public class Prontuario
    {
        Guid Id { get; set; }
        public required Pessoa Paciente { get; set; }
        public DateTime DataConsulta { get; set; }
        public int PressaoCardiacaMin { get; set; }
        public int PressaoCardiacaMax { get; set; }
        public double AlturaMt { get; set; }
        public double PesoKg { get; set; }
        public string? ResultadoImc { get; set; }
        public string? AvaliacaoMedica { get; set; }


        public Prontuario Cardio() 
        {
            var random = new Random();

            int cardio1 = random.Next(6, 21);
            int cardio2 = random.Next(6, 21);

            // Encontrando o mínimo e o máximo
            PressaoCardiacaMin = Math.Min(cardio1, cardio2);
            PressaoCardiacaMax = Math.Max(cardio1, cardio2);

            return this;
        }

        public Prontuario Imc()
        {
            var random = new Random();
            AlturaMt = NextDoubleInclusive(random,
                    Paciente.Sexo == Sexo.Masculino ? 1.45 : 1.40,
                    Paciente.Sexo == Sexo.Masculino ? 2.37 : 2.07);

            PesoKg = NextDoubleInclusive(random,
                    Paciente.Sexo == Sexo.Masculino ? 60 : 40,
                    Paciente.Sexo == Sexo.Masculino ? 360 : 210);

            double imc = PesoKg / (AlturaMt * AlturaMt);

            string diagnosticoImc;

            if (imc < 18.5)
                diagnosticoImc = "Abaixo do peso";
            else if (imc < 24.9)
                diagnosticoImc = "Peso normal";
            else if (imc < 29.9)
                diagnosticoImc = "Sobrepeso";
            else if (imc < 34.9)
                diagnosticoImc = "Obesidade grau 1";
            else if (imc < 39.9)
                diagnosticoImc = "Obesidade grau 2";
            else
                diagnosticoImc = "Obesidade grau 3, mórbida";

            ResultadoImc = $"IMC de {imc:F2} ({diagnosticoImc})";

            return this;
        }

        private static double NextDoubleInclusive(Random random, double min, double max)
        {
            double value = random.NextDouble();

            // Escala o valor aleatório para o intervalo [min, max]
            return min + (value * (max - min));
        }
    }


}
