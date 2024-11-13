import pandas as pd
import re
from datetime import datetime
from tkinter import Tk, filedialog, simpledialog, messagebox
from tqdm import tqdm


class ArchivoObs:
    """
    Clase que representa un archivo de observaciones (Obs).
    """
    columnas_requeridas = [
    "Cuenta", "REGION", "Zona", "Nombre", "Dirección", "Num1", "Num2", "Municipio",
    "Localidad", "Barrio", "Ciclo", "Tarifia", "Flag Ina", "MR_RTE_CD", "SECUENCIA",
    "SIREC_SEGMENTO", "NUMERO_PLACA", "FECHA_OBSERVACION_NO_TRABAJADA",
    "OBSERVACIONES_NO_TRABAJADAS", "CONS_EAT_M3", "Q_SEGMENTO", "TOTAL",
    "Q_PESO_OBSERVACIONES", "Q_BIMESTRAL_PESO", "Q_BAJO_PESO", "Q_PROMEDIO_1_PESO",
    "Q_PROMEDIO_2_PESO", "Q_ANUAL_PESO", "Q_PESO_SIREC", "Q_PESO_%_DESVIO",
    "Q_PESO_KWH_DESVIO", "Estado Cta", "CONTRATISTA_DIME", "FECHA_PEOR_SCORING_HISTORICO_SIN_TRABAJAR",
    "SCORING_PEOR_HISTORICO_SIN_TRABAJAR", "DESVIO_BIMESTRAL", "DESVIO_ANUAL", "Segmentacion"
]


    def __init__(self, ruta):
        self.ruta = ruta
        self.nombre_archivo = ruta.split("/")[-1]
        self.fecha_bajada = self.extraer_fecha_bajada()
        self.hoja = None
        self.data = None

    def extraer_fecha_bajada(self):
        """
        Extrae la fecha de bajada del nombre del archivo.
        """
        match = re.search(r'\b\d{8}\b', self.nombre_archivo)
        if match:
            fecha_str = match.group(0)
            return datetime.strptime(fecha_str, "%d%m%Y")  # Devuelve un datetime
        else:
            raise ValueError(f"No se encontró una fecha válida en el archivo: {self.nombre_archivo}")

    def cargar_hoja(self):
        """
        Permite seleccionar la hoja del archivo y la carga.
        """
        self.hoja = simpledialog.askstring("Nombre de Hoja", f"Ingrese el nombre de la hoja para {self.nombre_archivo}:")
        if not self.hoja:
            raise ValueError(f"No seleccionaste la hoja para el archivo: {self.nombre_archivo}")

        self.data = pd.read_excel(self.ruta, sheet_name=self.hoja)

    def filtrar_columnas(self):
        """
        Filtra las columnas requeridas en el archivo Obs.
        """
        if self.data is None:
            raise ValueError("El archivo no ha sido cargado correctamente.")

        for col in self.columnas_requeridas:
            if col not in self.data.columns:
                raise ValueError(f"La columna {col} no está en el archivo: {self.nombre_archivo}")

        self.data = self.data[self.columnas_requeridas]
        self.data['FECHA_BAJADA'] = self.fecha_bajada


class Reporte10:
    """
    Clase que representa el archivo de Reporte 10.
    """
    def __init__(self, ruta):
        self.ruta = ruta
        self.data = pd.read_excel(ruta)


class Procesador:
    """
    Clase encargada de procesar los archivos Obs y cruzarlos con el Reporte 10.
    """
    def __init__(self, rutas_obs, reporte_10):
        self.archivos_obs = [ArchivoObs(ruta) for ruta in rutas_obs]
        self.reporte_10 = Reporte10(reporte_10)
        self.resultado = None

    def procesar(self):
        """
        Procesa los archivos Obs y los cruza con el Reporte 10.
        """
        datos_validos = []

        for archivo_obs in tqdm(self.archivos_obs, desc="Procesando archivos Obs"):
            try:
                # Cargar y filtrar el archivo Obs
                archivo_obs.cargar_hoja()
                archivo_obs.filtrar_columnas()

                # Cruzar con el reporte 10
                df_cruce = pd.merge(
                    archivo_obs.data,
                    self.reporte_10.data,
                    on="Cuenta",  # Ajustar según las columnas comunes
                    how="inner"
                )

                # Filtrar por fechas válidas (FEC_TRABAJO > FECHA_BAJADA)
                df_cruce = df_cruce[df_cruce['FEC_TRABAJO'] > archivo_obs.fecha_bajada]
                
            
            

                # Guardar los datos válidos
                datos_validos.append(df_cruce)

            except Exception as e:
                print(f"Error procesando {archivo_obs.nombre_archivo}: {e}")

        # Concatenar los datos válidos
        if datos_validos:
            self.resultado = pd.concat(datos_validos, ignore_index=True)
        else:
            raise ValueError("No se encontraron datos válidos después del procesamiento.")

    def guardar_resultado(self, ruta_salida):
        """
        Guarda el resultado del procesamiento en un archivo Excel.
        """
        if self.resultado is None:
            raise ValueError("No hay resultados para guardar.")

        self.resultado.to_excel(ruta_salida, index=False)


class Aplicacion:
    """
    Clase principal que coordina la ejecución del programa.
    """
    def __init__(self):
        self.rutas_obs = None
        self.ruta_reporte10 = None

    def seleccionar_archivos(self):
        """
        Permite seleccionar los archivos Obs y el Reporte 10.
        """
        Tk().withdraw()

        # Seleccionar el reporte 10
        self.ruta_reporte10 = filedialog.askopenfilename(
            title="Seleccionar el archivo del Reporte 10",
            filetypes=[("Archivos Excel", "*.xlsx")]
        )
        if not self.ruta_reporte10:
            raise ValueError("No seleccionaste el archivo del Reporte 10.")

        # Seleccionar los archivos Obs
        self.rutas_obs = filedialog.askopenfilenames(
            title="Seleccionar archivos Obs",
            filetypes=[("Archivos Excel", "*.xlsx")]
        )
        if not self.rutas_obs:
            raise ValueError("No seleccionaste ningún archivo Obs.")

    def ejecutar(self):
        """
        Ejecuta el flujo completo de procesamiento.
        """
        try:
            # Seleccionar archivos
            self.seleccionar_archivos()

            # Procesar los archivos
            procesador = Procesador(self.rutas_obs, self.ruta_reporte10)
            procesador.procesar()

            # Guardar el resultado
            ruta_salida = filedialog.asksaveasfilename(
                title="Guardar archivo de resultado",
                defaultextension=".xlsx",
                filetypes=[("Archivos Excel", "*.xlsx")]
            )
            if not ruta_salida:
                raise ValueError("No seleccionaste una ruta para guardar el archivo.")

            procesador.guardar_resultado(ruta_salida)
            messagebox.showinfo("Éxito", f"Archivo guardado exitosamente en: {ruta_salida}")

        except Exception as e:
            messagebox.showerror("Error", f"Ocurrió un error: {e}")


if __name__ == "__main__":
    app = Aplicacion()
    app.ejecutar()