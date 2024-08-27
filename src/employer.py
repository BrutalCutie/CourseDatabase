class Employer:

    def __init__(self, employer_data: tuple):

        (self.emp_id,
         self.emp_name,
         self.emp_open_vac) = employer_data

    def __str__(self):
        return (
            f"| {self.emp_id:^15} | {self.emp_name:^40} | {self.emp_open_vac:^10}|"
        )

