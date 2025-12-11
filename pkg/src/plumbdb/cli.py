import json
import pathlib

import click


@click.group()
def cli():
    pass


# TODO: input file type can be better defined
@cli.command(name="download-pdb", help="Download PDB files from the PDB database.")
@click.option("-i", "--input-file", type=str, required=True)
@click.option(
    "output_directory",
    "-o",
    "--output-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
)
def download_pdb_structure(input_file, output_directory):
    from asapdiscovery.data.services.rcsb.rcsb_download import download_pdb_structure

    output_directory.mkdir(exist_ok=True, parents=True)
    with open(input_file, "r") as f:
        record_dict = json.load(f)
    downloaded = download_pdb_structure(
        record_dict["pdb_id"], output_directory, file_format="cif1"
    )
    record_dict["cif"] = downloaded
    with open(output_directory / "record.json", "w") as f:
        json.dump(record_dict, f)


@cli.command(
    "assess-prepped-protein",
    help="Assess the quality of the prepped protein structure.",
)
@click.option(
    "output_directory",
    "-o",
    "--output-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
)
@click.option("input_openeye_du", "-i", "--input-file", type=str, required=True)
def assess_prepped_protein(output_directory, input_openeye_du):
    try:
        from openeye import oespruce
    except ModuleNotFoundError:
        raise click.UsageError("Could not import openeye")

    from asapdiscovery.data.backend.openeye import load_openeye_design_unit

    output_directory.mkdir(exist_ok=True, parents=True)

    du = load_openeye_design_unit(input_openeye_du)

    # should use a different id method
    stem = pathlib.Path(input_openeye_du).stem
    validator = oespruce.OEValidateDesignUnit()
    err_msgs = validator.GetMessages(validator.Validate(du))
    sq = du.GetStructureQuality()

    report = {
        "errors": err_msgs,
        "warnings": [],
        "has_iridium_data": sq.HasIridiumData(),
    }

    with open(output_directory / f"{stem}_quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

# Prep for docking
from typing import TYPE_CHECKING, Optional
# copying everything
# TODO delete everything from here that isn't needed

def postera(func):
    return click.option(
        "--postera",
        is_flag=True,
        default=False,
        help="Whether to download complexes from Postera.",
    )(func)


def postera_molset_name(func):
    return click.option(
        "--postera-molset-name",
        type=str,
        default=None,
        help="The name of the Postera molecule set to use.",
    )(func)


def postera_upload(func):
    return click.option(
        "--postera-upload",
        is_flag=True,
        default=False,
        help="Whether to upload results to Postera.",
    )(func)


def postera_args(func):
    return postera(postera_molset_name(postera_upload(func)))


def use_dask(func):
    return click.option(
        "--use-dask",
        is_flag=True,
        default=False,
        help="Whether to use dask for parallelism.",
    )(func)


def dask_type(func):
    return click.option(
        "--dask-type",
        type=click.Choice(DaskType.get_values(), case_sensitive=False),
        default=DaskType.LOCAL,
        help="The type of dask cluster to use. Local mode is reccommended for most use cases.",
    )(func)


def failure_mode(func):
    return click.option(
        "--failure-mode",
        type=click.Choice(FailureMode.get_values(), case_sensitive=False),
        default=FailureMode.SKIP,
        help="The failure mode for dask. Can be 'raise' or 'skip'.",
        show_default=True,
    )(func)


def dask_n_workers(func):
    return click.option(
        "--dask-n-workers",
        type=int,
        default=None,
        help="The number of workers to use with dask.",
    )(func)


def dask_args(func):
    return use_dask(dask_type(dask_n_workers(failure_mode(func))))


def target(func):
    from asapdiscovery.data.services.postera.manifold_data_validation import TargetTags
    return click.option(
        "--target",
        type=click.Choice(TargetTags.get_values(), case_sensitive=True),
        help="The target for the workflow",
        required=True,
    )(func)


def ligands(func):
    return click.option(
        "-l",
        "--ligands",
        type=click.Path(resolve_path=True, exists=True, file_okay=True, dir_okay=False),
        help="File containing ligands",
    )(func)


def output_dir(func):
    return click.option(
        "--output-dir",
        type=click.Path(
            resolve_path=True, exists=False, file_okay=False, dir_okay=True
        ),
        help="The directory to output results to.",
        default="output",
    )(func)


def overwrite(func):
    return click.option(
        "--overwrite/--no-overwrite",
        default=True,
        help="Whether to overwrite the output directory if it exists.",
    )(func)


def input_json(func):
    return click.option(
        "--input-json",
        type=click.Path(resolve_path=True, exists=True, file_okay=True, dir_okay=False),
        help="Path to a json file containing the inputs to the workflow,  WARNING: overrides all other inputs.",
    )(func)

# flag to run all ml scorers
def ml_score(func):
    return click.option(
        "--ml-score",
        is_flag=True,
        default=True,
        help="Whether to run all ml scorers",
    )(func)


def fragalysis_dir(func):
    return click.option(
        "--fragalysis-dir",
        type=click.Path(resolve_path=True, exists=True, file_okay=False, dir_okay=True),
        help="Path to a directory containing fragments to dock.",
    )(func)


def structure_dir(func):
    return click.option(
        "--structure-dir",
        type=click.Path(resolve_path=True, exists=True, file_okay=False, dir_okay=True),
        help="Path to a directory containing structures.",
    )(func)


def pdb_file(func):
    return click.option(
        "--pdb-file",
        type=click.Path(resolve_path=True, exists=True, file_okay=True, dir_okay=False),
        help="Path to a pdb file containing a structure",
    )(func)


def cache_dir(func):
    return click.option(
        "--cache-dir",
        type=click.Path(
            resolve_path=True, exists=False, file_okay=False, dir_okay=True
        ),
        help="Path to a directory where design units are cached.",
    )(func)


def use_only_cache(func):
    return click.option(
        "--use-only-cache",
        is_flag=True,
        default=False,
        help="Whether to only use the cache.",
    )(func)


def gen_cache_w_default(func):
    return click.option(
        "--gen-cache",
        type=click.Path(
            resolve_path=False, exists=False, file_okay=False, dir_okay=True
        ),
        help="Path to a directory where a design unit cache should be generated.",
        default="prepped_structure_cache",
    )(func)


def md(func):
    return click.option(
        "--md",
        is_flag=True,
        default=False,
        help="Whether to run MD",
    )(func)


def md_steps(func):
    return click.option(
        "--md-steps",
        type=int,
        default=2500000,
        help="Number of MD steps",
    )(func)

def core_smarts(func):
    return click.option(
        "-cs",
        "--core-smarts",
        type=click.STRING,
        help="The SMARTS which should be used to select which atoms to constrain to the reference structure.",
    )(func)


def save_to_cache(func):
    return click.option(
        "--save-to-cache/--no-save-to-cache",
        help="If the newly generated structures should be saved to the cache folder.",
        default=True,
    )(func)


def loglevel(func):
    return click.option(
        "--loglevel",
        type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
        help="The log level to use.",
        default="INFO",
        show_default=True,
    )(func)


def ref_chain(func):
    return click.option(
        "--ref-chain",
        type=str,
        default=None,
        help="Chain ID to align to in reference structure containing the active site.",
    )(func)


def active_site_chain(func):
    return click.option(
        "--active-site-chain",
        type=str,
        default=None,
        help="Active site chain ID to align to ref_chain in reference structure",
    )(func)

from asapdiscovery.data.util.dask_utils import DaskType, FailureMode

if TYPE_CHECKING:
    from asapdiscovery.data.services.postera.manifold_data_validation import TargetTags


@cli.command(
    "prep-protein-for-docking",
    help="Prep protein to make OE Design Units and corresponding schema.",
)
@target
@click.option(
    "--align",
    type=click.Path(resolve_path=True, exists=True, file_okay=True, dir_okay=False),
    help="Path to a reference structure to align to",
)
@ref_chain
@active_site_chain
@click.option(
    "--seqres-yaml",
    type=click.Path(resolve_path=True, exists=True, file_okay=True, dir_okay=False),
    help="Path to a seqres yaml file to mutate to, if not specified will use the default for the target",
)
@click.option(
    "--loop-db",
    type=click.Path(resolve_path=True, exists=True, file_okay=True, dir_okay=False),
    help="Path to a loop database to use for prepping",
)
@click.option(
    "--oe-active-site-residue",
    type=str,
    help="OE formatted string of active site residue to use if not ligand bound",
)
@pdb_file
@fragalysis_dir
@structure_dir
@click.option(
    "--cache-dir",
    help="The path to cached prepared complexes which can be used again.",
    type=click.Path(resolve_path=True, exists=True, file_okay=False, dir_okay=True),
)
@save_to_cache
@dask_args
@output_dir
@input_json
def protein_prep(
    target: "TargetTags",
    align: Optional[str] = None,
    ref_chain: Optional[str] = None,
    active_site_chain: Optional[str] = None,
    seqres_yaml: Optional[str] = None,
    loop_db: Optional[str] = None,
    oe_active_site_residue: Optional[str] = None,
    pdb_file: Optional[str] = None,
    fragalysis_dir: Optional[str] = None,
    structure_dir: Optional[str] = None,
    cache_dir: Optional[str] = None,
    save_to_cache: bool = True,
    use_dask: bool = False,
    dask_type: DaskType = DaskType.LOCAL,
    dask_n_workers: Optional[int] = None,
    failure_mode: FailureMode = FailureMode.SKIP,
    output_dir: str = "output",
    input_json: Optional[str] = None,
):
    """
    Run protein prep on a set of structures.
    """
    from asapdiscovery.workflows.prep_workflows.protein_prep import (
        ProteinPrepInputs,
        protein_prep_workflow,
    )

    if input_json is not None:
        print("Loading inputs from json file... Will override all other inputs.")
        inputs = ProteinPrepInputs.from_json_file(input_json)

    else:
        inputs = ProteinPrepInputs(
            target=target,
            align=align,
            ref_chain=ref_chain,
            active_site_chain=active_site_chain,
            seqres_yaml=seqres_yaml,
            loop_db=loop_db,
            oe_active_site_residue=oe_active_site_residue,
            pdb_file=pdb_file,
            fragalysis_dir=fragalysis_dir,
            structure_dir=structure_dir,
            cache_dir=cache_dir,
            save_to_cache=save_to_cache,
            use_dask=use_dask,
            dask_type=dask_type,
            dask_n_workers=dask_n_workers,
            failure_mode=failure_mode,
            output_dir=output_dir,
        )

    protein_prep_workflow(inputs)

# TODO: check for openeye installation, maybe make it a decorator
@cli.command(
    "generate-constrained-ligand-poses",
    help="Generate constrained ligand poses using OpenEye tooling.",
)
@click.option("input_sdf", "-i", "--input-sdf", required=True, type=str)
@click.option("prepped_schema", "-s", "--prepped-schema", type=str)
@click.option(
    "output_directory",
    "-o",
    "--output-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
)
def generate_constrained_ligand_poses(input_sdf, prepped_schema, output_directory):
    try:
        from asapdiscovery.data.schema.complex import PreppedComplex
        from asapdiscovery.data.readers.molfile import MolFileFactory
        from asapdiscovery.data.schema.ligand import Ligand
        from asapdiscovery.docking.schema.pose_generation import (
            OpenEyeConstrainedPoseGenerator,
        )
        from asapdiscovery.data.backend.openeye import save_openeye_sdfs
    except ModuleNotFoundError:
        raise click.UsageError("Could not import openeye")

    raw_ligands = MolFileFactory(filename=input_sdf).load()

    # reconstruct ligands from smiles because of some weirdness with the sdf files
    ligs = [
        Ligand.from_smiles(
            compound_name=lig.tags["BindingDB monomerid"],
            smiles=lig.smiles,
            tags=lig.dict(),
        )
        for lig in raw_ligands
    ]

    prepped_complex = PreppedComplex.parse_file(prepped_schema)

    poser = OpenEyeConstrainedPoseGenerator()
    poses = poser.generate_poses(prepped_complex, ligands=ligs)
    oemols = [ligand.to_oemol() for ligand in poses.posed_ligands]
    # save to sdf file
    save_openeye_sdfs(oemols, output_directory / "poses.sdf")


@cli.command("prep-cif")
@click.option("input_json", "-j", "--input-json", type=str, required=True)
@click.option("input_cif", "-c", "--input-cif", type=str, required=True)
@click.option(
    "fasta_sequence", "-f", "--fasta-sequence", type=str, default=None, required=True
)
@click.option("loop_db", "--loopdb", type=str, required=True)
@click.option(
    "output_directory",
    "-o",
    "--output-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    default="./",
    required=True,
)
def prep_cif(input_json, input_cif, fasta_sequence, loop_db, output_directory):
    from asapdiscovery.data.backend.openeye import load_openeye_cif1
    from asapdiscovery.modeling.modeling import split_openeye_mol
    from asapdiscovery.data.schema.ligand import Ligand
    from asapdiscovery.data.schema.complex import Complex
    from plumbdb.oespruce import spruce_protein
    from asapdiscovery.data.schema.target import Target

    output_directory.mkdir(exist_ok=True, parents=True)

    with open(input_json, "r") as f:
        record_dict = json.load(f)

    graphmol = load_openeye_cif1(input_cif)

    # this is what you would do if you didn't want to use whatever ligand is in the protein
    # split_dict = split_openeye_mol(graphmol, keep_one_lig=False)
    split_dict = split_openeye_mol(graphmol, keep_one_lig=True)

    # # Save initial protein as pdb file
    target = Target.from_oemol(
        split_dict["prot"],
        target_name=record_dict["pdb_id"],
    )
    # target.to_pdb("protein.pdb")

    # this is what you would do if you didn't want to use whatever ligand is in the protein
    ligand = Ligand.from_oemol(split_dict["lig"])
    # ligand = Ligand.from_sdf(f'{record_dict["compound_name"]}.sdf')

    combined_complex = Complex(target=target, ligand=ligand, ligand_chain="L")

    oemol = combined_complex.to_combined_oemol()

    results, spruced = spruce_protein(
        initial_prot=oemol,
        protein_sequence=fasta_sequence,
        loop_db=loop_db,
    )

    split_dict = split_openeye_mol(spruced)
    prepped_target = Target.from_oemol(split_dict["prot"], **target.dict())
    prepped_ligand = Ligand.from_oemol(split_dict["lig"], **ligand.dict())
    prepped_ligand.to_sdf(output_directory / f"{ligand.compound_name}_ligand.sdf")
    prepped_target.to_pdb(output_directory / f"{target.target_name}_spruced.pdb")

    prepped_complex = Complex(
        target=prepped_target, ligand=prepped_ligand, ligand_chain="L"
    )

    filename = f"{target.target_name}_{ligand.compound_name}_spruced_complex.pdb"
    prepped_complex.to_pdb(output_directory / filename)
    results.to_json_file(output_directory / f"{target.target_name}_spruce_results.json")


# TODO: help string
@cli.command(
    "process-bindingdb", help="Parse and verify SDF files downloaded from bindingdb."
)
@click.option(
    "input_directory",
    "-i",
    "--input-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
    help="SDF file to process",
)
@click.option(
    "output_directory",
    "-o",
    "--output-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
    help="Directory to write output files",
)
def process_bindingdb(input_directory, output_directory):
    from asapdiscovery.data.schema.ligand import Ligand
    from asapdiscovery.data.readers.molfile import MolFileFactory
    import pandas as pd
    import math

    output_directory.mkdir(exist_ok=True, parents=True)

    # get all sdf files
    sdfs = list(input_directory.glob("*3D.sdf"))

    output = []
    for sdf in sdfs:
        # asap function to read separate ligands from a multi-ligand sdf file
        mols: list[Ligand] = MolFileFactory(filename=sdf).load()

        # create a dictionary for each ligand containing various relevant information
        # there are some hidden choices here, for instance OpenEye is adding hydrogens which you might not want

        for mol in mols:
            mol_dict = {
                "compound_name": mol.compound_name,
                "filename": sdf.name,
                "has_3d": mol.to_oemol().GetDimension() == 3,
                "num_atoms": mol.to_oemol().NumAtoms(),
                "smiles": mol.smiles,
                # "pdb_id": mol.tags.get("PDB ID")[:4] # removed trailing space
                # if mol.tags.get("PDB ID") # removed trailing space
                "pdb_id": mol.tags.get("PDB ID(s) for Ligand-Target Complex")[:4] # removed trailing space
                if mol.tags.get("PDB ID(s) for Ligand-Target Complex")
                else "",
            }

            # any data in the SDF file is saved to the 'tags' attribute of an asapdiscovery Ligand object
            mol_dict.update(mol.tags)

            # write out sdf file
            if mol_dict["has_3d"]:
                mol.to_sdf(output_directory / f"{mol.compound_name}.sdf")

            output.append(mol_dict)

    df = pd.DataFrame.from_records(output)
    df.to_csv(output_directory / "processed_bindingdb.csv", index=False)

    # write separate csvs for 2D and 3D
    df_2d = df[~df["has_3d"]]
    df_3d = df[df["has_3d"]]
    df_2d.to_csv(output_directory / "2d_bindingdb.csv", index=False)
    df_3d.to_csv(output_directory / "3d_bindingdb.csv", index=False)

    unique_sdf_filenames = df_3d["filename"].unique()
    with open(output_directory / "unique_3D_sdf_filenames.txt", "w") as f:
        for filename in unique_sdf_filenames:
            f.write(f"{filename}\n")

    # write out separate json records
    for record in df_3d.to_dict(orient="records"):
        with open(output_directory / f"{record['compound_name']}.json", "w") as f:
            f.write(
                json.dumps(
                    record, indent=4, default=lambda x: None if math.isnan(x) else x
                )
            )

@cli.command("rename-ligands-for-openfe")
@click.option("-i",
              "--input-sdf",
              type=click.Path(file_okay=True, dir_okay=False, path_type=pathlib.Path),)
@click.option(
    "-o",
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
    default=pathlib.Path("./"),
    help="Path to the output directory where the results will be stored",
)
def rename_ligands_for_sdf(input_sdf, output_dir):
    from asapdiscovery.data.schema.ligand import Ligand
    from asapdiscovery.data.readers.molfile import MolFileFactory
    from asapdiscovery.data.schema.ligand import write_ligands_to_multi_sdf
    mols: list[Ligand] = MolFileFactory(filename=input_sdf).load()
    new_ligands = []
    for mol in mols:
        oemol = mol.to_oemol()
        oemol.SetTitle(mol.compound_name)
        new_ligands.append(Ligand.from_oemol(oemol))
    output_sdf = output_dir / "renamed.sdf"
    write_ligands_to_multi_sdf(output_sdf, new_ligands, overwrite=True)

@cli.command(
    "visualize-network",
    help="Generate a network plot of the proposed alchemical network",
)
@click.option(
    "network_graphml",
    "-n",
    "--network-graphml",
    type=str,
    required=True,
    help="Path to the input JSON file containing the atom mapping network.",
)
@click.option(
    "output_directory",
    "-o",
    "--output-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    required=True,
    default=pathlib.Path("./"),
    help="Path to the output directory where the results will be stored",
)
def visualize_network(network_graphml, output_directory):
    from openfe.utils.atommapping_network_plotting import plot_atommapping_network
    from openfe.setup import LigandNetwork

    output_directory.mkdir(exist_ok=True, parents=True)
    ligand_network = network_graphml

    if not ligand_network.exists():
        raise FileNotFoundError(
            f"Could not find the ligand network file at {ligand_network}"
        )

    with open(ligand_network) as f:
        graphml = f.read()

    network = LigandNetwork.from_graphml(graphml)
    fig = plot_atommapping_network(network)
    fig.savefig(output_directory / "network_plot.png")
